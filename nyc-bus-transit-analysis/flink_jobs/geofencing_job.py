import os
import json
import logging
import math
from datetime import datetime, timezone
from dotenv import load_dotenv
import requests


from pyflink.common import SimpleStringSchema, WatermarkStrategy, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeContext
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor

from zones import ZONES

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("geofencing-job")

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = "vehicle-positions"
KAFKA_CONSUMER_GROUP = "flink-geofencing"

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = os.environ.get("CLICKHOUSE_PORT")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DB")


DWELL_THRESHOLD_SECONDS = 120
DWELL_REPEAT_SECONDS = 60
ALERT_THRESHOLD_SECONDS = 30 * 60


def haversine_distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:

    R = 6371000  # radijus Zemlje u metrima
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = (math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2)
    
    return 2 * R * math.asin(math.sqrt(a))


def zones_containing_point(lat: float, lon: float) -> list[str]:

    result = []
    for zone in ZONES:
        distance = haversine_distance_m(lat, lon, zone["lat"], zone["lon"])
        if distance <= zone["radius_m"]:
            result.append(zone["zone_id"])
    
    return result


class GeofencingFunction(KeyedProcessFunction):

    def open(self, runtime_context: RuntimeContext):

        self.current_zones_state = runtime_context.get_map_state(
            MapStateDescriptor("current_zones", Types.STRING(), Types.LONG())
        )
        
        self.last_dwell_emit_state = runtime_context.get_map_state(
            MapStateDescriptor("last_dwell_emit", Types.STRING(), Types.LONG())
        )

        self.last_exited_zone_state = runtime_context.get_state(
            ValueStateDescriptor("last_exited_zone", Types.STRING())
        )
        
        self.alert_emitted_state = runtime_context.get_map_state(
            MapStateDescriptor("alert_emitted", Types.STRING(), Types.LONG())
        )

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        record = json.loads(value)

        lat = record.get("latitude")
        lon = record.get("longitude")

        if lat is None or lon is None:
            return

        vehicle_id = record.get("vehicle_id") or "unknown"
        route_id = record.get("route_id") or ""
        event_timestamp_s = record.get("timestamp")

        if event_timestamp_s:
            event_time_ms = int(event_timestamp_s) * 1000
        else:
            event_time_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        zones_now = set(zones_containing_point(lat, lon))

        zones_before = set(self.current_zones_state.keys())

        entered_zones = zones_now - zones_before
        for zone_id in entered_zones:

            yield from self._emit_event(
                event_time_ms, vehicle_id, route_id, zone_id,
                "ENTER", lat, lon, dwell_seconds=None,
            )
            self.current_zones_state.put(zone_id, event_time_ms)

            #TRANSITION ako postoji prethodno napustena zona
            last_exited_raw = self.last_exited_zone_state.value()
            if last_exited_raw is not None:
                from_zone_id, exit_time_ms_str = last_exited_raw.split("|")
                exit_time_ms = int(exit_time_ms_str)

                if from_zone_id != zone_id:
                    travel_time_seconds = int((event_time_ms - exit_time_ms) // 1000)
                    yield from self._emit_transition(
                        event_time_ms, vehicle_id, route_id,
                        from_zone_id, zone_id, travel_time_seconds,
                    )
                self.last_exited_zone_state.clear()

        # EXIT za zone u kojima je vozilo bilo pre, ali nije sada
        exited_zones = zones_before - zones_now #trajanje posete
        for zone_id in exited_zones:
            entry_time_ms = self.current_zones_state.get(zone_id)
            total_dwell_seconds = int((event_time_ms - entry_time_ms) // 1000)

            yield from self._emit_event(
                event_time_ms, vehicle_id, route_id, zone_id,
                "EXIT", lat, lon, dwell_seconds=total_dwell_seconds,
            )

            if (total_dwell_seconds >= ALERT_THRESHOLD_SECONDS
                    and self.alert_emitted_state.get(zone_id) is None):
                yield from self._emit_event(
                    event_time_ms, vehicle_id, route_id, zone_id,
                    "ALERT", lat, lon, dwell_seconds=total_dwell_seconds,
                )

            self.current_zones_state.remove(zone_id)
            self.last_dwell_emit_state.remove(zone_id)
            self.alert_emitted_state.remove(zone_id)
            self.last_exited_zone_state.update(f"{zone_id}|{event_time_ms}")

        # DWELL: zone u kojima se vozilo nalazi
        still_in_zones = zones_now & zones_before
        for zone_id in still_in_zones:
            entry_time_ms = self.current_zones_state.get(zone_id)
            dwell_seconds = int((event_time_ms - entry_time_ms) // 1000)

            #ALERT emituje se prvi put kad se predje prag
            if (dwell_seconds >= ALERT_THRESHOLD_SECONDS
                    and self.alert_emitted_state.get(zone_id) is None):
                yield from self._emit_event(
                    event_time_ms, vehicle_id, route_id, zone_id,
                    "ALERT", lat, lon, dwell_seconds=dwell_seconds,
                )
                self.alert_emitted_state.put(zone_id, 1)

            if dwell_seconds < DWELL_THRESHOLD_SECONDS:
                continue 

            last_emit = self.last_dwell_emit_state.get(zone_id)
            if last_emit is not None:
                seconds_since_last_emit = (event_time_ms - last_emit) // 1000
                if seconds_since_last_emit < DWELL_REPEAT_SECONDS:
                    continue  # vec je emitovan DWELL za ovu zonu

            yield from self._emit_event(
                event_time_ms, vehicle_id, route_id, zone_id,
                "DWELL", lat, lon, dwell_seconds=dwell_seconds,
            )
            self.last_dwell_emit_state.put(zone_id, event_time_ms)

    def _emit_event(self, event_time_ms, vehicle_id, route_id, zone_id, event_type, lat, lon, dwell_seconds):
        
        zone_name = next((z["zone_name"] for z in ZONES if z["zone_id"] == zone_id), zone_id)

        result = {
            "_kind": "zone_event",
            "event_time_ms": event_time_ms,
            "vehicle_id": vehicle_id,
            "route_id": route_id,
            "zone_id": zone_id,
            "zone_name": zone_name,
            "event_type": event_type,
            "latitude": lat,
            "longitude": lon,
            "dwell_seconds": dwell_seconds,
        }
        yield json.dumps(result)

    def _emit_transition(self, event_time_ms, vehicle_id, route_id, from_zone_id, to_zone_id, travel_time_seconds):
        
        from_zone_name = next((z["zone_name"] for z in ZONES if z["zone_id"] == from_zone_id), from_zone_id)
        to_zone_name = next((z["zone_name"] for z in ZONES if z["zone_id"] == to_zone_id), to_zone_id)

        result = {
            "_kind": "zone_transition",
            "event_time_ms": event_time_ms,
            "vehicle_id": vehicle_id,
            "route_id": route_id,
            "from_zone_id": from_zone_id,
            "from_zone_name": from_zone_name,
            "to_zone_id": to_zone_id,
            "to_zone_name": to_zone_name,
            "travel_time_seconds": travel_time_seconds,
        }
        yield json.dumps(result)


def write_to_clickhouse(json_str: str):

    record = json.loads(json_str)
    kind = record.get("_kind")

    if kind == "zone_event":
        _insert_zone_event(record)
    elif kind == "zone_transition":
        _insert_zone_transition(record)
    else:
        logger.error("Nepoznat _kind u rezultatu: %s", record)

def _insert_zone_event(record: dict):

    event_time_str = datetime.fromtimestamp(record["event_time_ms"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    dwell_value = "NULL" if record["dwell_seconds"] is None else str(record["dwell_seconds"])

    query = f"""
    INSERT INTO {CLICKHOUSE_DATABASE}.zone_events
    (event_time, vehicle_id, route_id, zone_id, zone_name, event_type, latitude, longitude, dwell_seconds)
    VALUES
    ('{event_time_str}', '{record["vehicle_id"]}', '{record["route_id"]}',
     '{record["zone_id"]}', '{record["zone_name"]}', '{record["event_type"]}',
     {record["latitude"]}, {record["longitude"]}, {dwell_value})
    """
    _execute_insert(query)

def _insert_zone_transition(record: dict):
    transition_time_str = datetime.fromtimestamp(record["event_time_ms"] / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    query = f"""
    INSERT INTO {CLICKHOUSE_DATABASE}.zone_transitions
    (transition_time, vehicle_id, route_id, from_zone_id, from_zone_name,
     to_zone_id, to_zone_name, travel_time_seconds)
    VALUES
    ('{transition_time_str}', '{record["vehicle_id"]}', '{record["route_id"]}',
     '{record["from_zone_id"]}', '{record["from_zone_name"]}',
     '{record["to_zone_id"]}', '{record["to_zone_name"]}',
     {record["travel_time_seconds"]})
    """
    _execute_insert(query)

def _execute_insert(query: str):

    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
    response = requests.post(
        url,
        params={"database": CLICKHOUSE_DATABASE},
        auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
        data=query.encode("utf-8"),
        timeout=10,
    )
    if response.status_code != 200:
        logger.error("ClickHouse insert failed: %s | query=%s", response.text, query)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS)
        .set_topics(KAFKA_TOPIC)
        .set_group_id(KAFKA_CONSUMER_GROUP)
        .set_starting_offsets(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    stream = env.from_source(
        kafka_source,
        watermark_strategy=WatermarkStrategy.no_watermarks(),
        source_name="vehicle-positions-source",
    )

    #po vehicle_id da bi svako vozilo ima svoje stanje
    keyed_stream = stream.key_by(lambda v: json.loads(v).get("vehicle_id") or "unknown")

    events_stream = keyed_stream.process(GeofencingFunction(), output_type=Types.STRING())

    
    events_stream.print()
    events_stream.map(write_to_clickhouse)

    env.execute("Geofencing analiza - ENTER/EXIT/DWELL/ALERT/TRANSITION")


if __name__ == "__main__":
    main()