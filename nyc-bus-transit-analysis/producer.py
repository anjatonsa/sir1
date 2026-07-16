import os
import time
import json
import logging
import requests

from datetime import datetime, timezone
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2
from kafka import KafkaProducer
from kafka.errors import KafkaError

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("producer")

MTA_BUS_API_KEY = os.environ.get("MTA_BUS_API_KEY")

if not MTA_BUS_API_KEY:
    raise RuntimeError(
        "Nedostaje MTA_BUS_API_KEY environment varijabla. "
        "Potrebna je registracija na https://register.developer.obanyc.com/."
    )

GTFS_RT_URL = "https://gtfsrt.prod.obanyc.com/vehiclePositions"


KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_TOPIC = "vehicle-positions"

POLL_INTERVAL_SECONDS = 15


def create_producer(max_retries: int = 10, retry_delay_seconds: int = 5) -> KafkaProducer:
    last_error = None

    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks=1,
                retries=3,
            )
            logger.info("Uspesno povezan na Kafku (pokusaj %d/%d).", attempt, max_retries)
            return producer
        except Exception as e:
            last_error = e
            logger.warning(
                "Kafka kontjener nije spreman (pokusaj %d/%d): %s. Ceka se %ds...",
                attempt, max_retries, e, retry_delay_seconds,
            )
            time.sleep(retry_delay_seconds)

    raise RuntimeError(f"Povezivanje na Kafku nije uspelo nakon {max_retries} pokusaja.") from last_error


def fetch_feed() -> gtfs_realtime_pb2.FeedMessage:

    #preuzimanje i parsiranje GTFS-RT protobuf feed-a
    response = requests.get(
        GTFS_RT_URL,
        params={"key": MTA_BUS_API_KEY},
        timeout=10,
    )
    response.raise_for_status()

    logger.info(
        "HTTP %d | Velicina : %d bajtova",
        response.status_code,
        len(response.content),
    )

    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)

    num_vechicles = 0
    for entity in feed.entity:
        if entity.HasField("vehicle"):
            num_vechicles+= 1

    logger.info(
        "Feed sadrzi ukupno %s entiteta vozila.",
        num_vechicles,
    )

    return feed


def extract_vehicle_positions(feed: gtfs_realtime_pb2.FeedMessage) -> list[dict]:
    
    records = []

    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue

        vp = entity.vehicle

        record = {
            "entity_id": entity.id,
            "trip_id": vp.trip.trip_id if vp.HasField("trip") else None,
            "route_id": vp.trip.route_id if vp.HasField("trip") else None,
            "vehicle_id": vp.vehicle.id if vp.HasField("vehicle") else None,
            "latitude": vp.position.latitude if vp.HasField("position") else None,
            "longitude": vp.position.longitude if vp.HasField("position") else None,
            "bearing": vp.position.bearing if vp.position.HasField("bearing") else None,
            "current_status": vp.current_status if vp.HasField("current_status") else None,
            "timestamp": vp.timestamp if vp.HasField("timestamp") else None,
            "stop_id": vp.stop_id if vp.HasField("stop_id") else None,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }

        # filtriramo zapise bez koordinata jer nisu korisni za prostornu analizu
        if record["latitude"] is None or record["longitude"] is None:
            continue

        records.append(record)

    return records


def run():
    producer = create_producer()
    logger.info("Producer pokrenut. Kafka bootstrap: %s, topic: %s", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)

    while True:
        cycle_start = time.monotonic()

        try:
            feed = fetch_feed()
            records = extract_vehicle_positions(feed)

            for record in records:
                # kljuc poruke je vehicle_id ili trip_id kako bi poruke istog vozila zavrsile na istoj particiji 
                key = record.get("vehicle_id") or record.get("trip_id") or "unknown"
                producer.send(KAFKA_TOPIC, key=key, value=record)

            producer.flush()
            logger.info("Poslato %d vehicle position zapisa.", len(records))

        except requests.RequestException as e:
            logger.error("Greska pri preuzimanju feed-a: %s", e)
        except KafkaError as e:
            logger.error("Greska pri slanju u Kafku: %s", e)
        except Exception as e:
            logger.exception("Neocekivana greska: %s", e)

        elapsed = time.monotonic() - cycle_start
        sleep_time = max(0, POLL_INTERVAL_SECONDS - elapsed)
        time.sleep(sleep_time)


if __name__ == "__main__":
    run()