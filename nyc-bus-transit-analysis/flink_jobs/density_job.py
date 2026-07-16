import os
import json
import logging
import math
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
import requests


from pyflink.common import SimpleStringSchema, WatermarkStrategy, Types, Duration
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.time import Time
from pyflink.datastream.functions import ProcessWindowFunction

from zones import ZONES


load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("density-job")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = "vehicle-positions"
KAFKA_CONSUMER_GROUP = "flink-density"

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST")
CLICKHOUSE_PORT = os.environ.get("CLICKHOUSE_PORT")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD")
CLICKHOUSE_DATABASE = os.environ.get("CLICKHOUSE_DB")

WINDOW_SIZE_MINUTES = 5

WATERMARK_MAX_OUT_OF_ORDERNESS_SECONDS = 10
BATCH_SIZE = 50
BATCH_TIMEOUT_SECONDS = 5.0

def haversine_distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371000
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

class VehiclePositionTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value: str, record_timestamp: int) -> int:
        try:
            record = json.loads(value)
            event_timestamp_s = record.get("timestamp")
            if event_timestamp_s:
                return int(event_timestamp_s) * 1000
        except (json.JSONDecodeError, TypeError, ValueError):
            pass
        return record_timestamp


def extract_zone_vehicle_pairs(json_str: str):
    record = json.loads(json_str)

    lat = record.get("latitude")
    lon = record.get("longitude")
    if lat is None or lon is None:
        return

    vehicle_id = record.get("vehicle_id") or "unknown"
    route_id = record.get("route_id") or ""

    for zone_id in zones_containing_point(lat, lon):
        yield (zone_id, vehicle_id, route_id)


class CountDistinctVehiclesPerZone(ProcessWindowFunction):

    def process(self, key: str, context: "ProcessWindowFunction.Context", elements):
        vehicle_ids = set()
        route_ids = set()

        for zone_id, vehicle_id, route_id in elements:
            vehicle_ids.add(vehicle_id)
            if route_id:
                route_ids.add(route_id)

        window_end_ms = context.window().end
        zone_name = next((z["zone_name"] for z in ZONES if z["zone_id"] == key), key)

        result = {
            "window_end_ms": window_end_ms,
            "zone_id": key,
            "zone_name": zone_name,
            "vehicle_count": len(vehicle_ids),
            "route_ids": sorted(route_ids),
        }
        yield json.dumps(result)


def _format_density_row(record: dict) -> str:
    window_end_str = datetime.fromtimestamp(
        record["window_end_ms"] / 1000, tz=timezone.utc
    ).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

    route_ids_literal = "[" + ",".join(f"'{r}'" for r in record["route_ids"]) + "]"

    return (
        f"('{window_end_str}', '{record['zone_id']}', '{record['zone_name']}', "
        f"{record['vehicle_count']}, {route_ids_literal})"
    )


class BatchClickHouseDensitySink:

    def __init__(self):
        self._session = None
        self._buffer = []
        self._last_flush_time = time.monotonic()

    def __call__(self, json_str: str):
        if self._session is None:
            self._session = requests.Session()

        record = json.loads(json_str)
        self._buffer.append(_format_density_row(record))

        should_flush = (
            len(self._buffer) >= BATCH_SIZE
            or (time.monotonic() - self._last_flush_time) >= BATCH_TIMEOUT_SECONDS
        )
        if should_flush:
            self._flush()

    def _flush(self):
        if not self._buffer:
            self._last_flush_time = time.monotonic()
            return

        values_clause = ",\n".join(self._buffer)
        query = f"""
        INSERT INTO {CLICKHOUSE_DATABASE}.zone_density
        (window_end, zone_id, zone_name, vehicle_count, route_ids)
        VALUES
        {values_clause}
        """

        url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
        response = self._session.post(
            url,
            params={"database": CLICKHOUSE_DATABASE},
            auth=(CLICKHOUSE_USER, CLICKHOUSE_PASSWORD),
            data=query.encode("utf-8"),
            timeout=10,
        )
        if response.status_code != 200:
            logger.error("ClickHouse batch insert failed: %s", response.text)
        else:
            logger.info("Batch insert: %d zone_density redova upisano.", len(self._buffer))

        self._buffer = []
        self._last_flush_time = time.monotonic()


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

    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_seconds(WATERMARK_MAX_OUT_OF_ORDERNESS_SECONDS))
        .with_timestamp_assigner(VehiclePositionTimestampAssigner())
    )

    stream = env.from_source(
        kafka_source,
        watermark_strategy=watermark_strategy,
        source_name="vehicle-positions-source",
    )

    zone_vehicle_pairs = stream.flat_map(
        extract_zone_vehicle_pairs,
        output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.STRING()]),
    )

    density_stream = (
        zone_vehicle_pairs
        .key_by(lambda t: t[0])  # kljuc = zone_id
        .window(TumblingEventTimeWindows.of(Time.minutes(WINDOW_SIZE_MINUTES)))
        .process(CountDistinctVehiclesPerZone(), output_type=Types.STRING())
    )

    density_stream.print()
    density_stream.map(BatchClickHouseDensitySink())

    env.execute(f"Gustina vozila po zoni - event time tumbling window {WINDOW_SIZE_MINUTES}min")


if __name__ == "__main__":
    main()