CONFIG_FILE=".env.${1}"

if [ ! -f "$CONFIG_FILE" ]; then
  echo "Greška. Fajl $CONFIG_FILE ne postoji."
  exit 1
fi

set -a
source .env
source "$CONFIG_FILE"
set +a

echo "Pokretanje konfiguracije: $1"
echo "  TaskManager-a:  $NUM_TASK_MANAGERS"
echo "  Memorija po TM: $TASKMANAGER_MEMORY"
echo "  Paralelizam:    $FLINK_PARALLELISM"
echo "  Kafka particija: $KAFKA_NUM_PARTITIONS"

docker compose --env-file .env down

docker compose --env-file .env up -d kafka clickhouse superset kafka-ui

until docker compose --env-file .env exec kafka bash -c \
  "/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list" > /dev/null 2>&1; do
  echo "  Kafka not ready..."
  sleep 5
done

docker compose --env-file .env exec kafka bash -c \
  "/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --delete --topic vehicle-positions 2>/dev/null"
sleep 5

docker compose --env-file .env exec kafka bash -c \
  "/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --create --topic vehicle-positions \
  --partitions $KAFKA_NUM_PARTITIONS \
  --replication-factor 1"

echo "Topic vehicle-positions kreiran sa $KAFKA_NUM_PARTITIONS particija."

docker compose --env-file .env exec kafka bash -c \
  "/opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 \
  --describe --topic vehicle-positions"

docker compose --env-file .env up -d flink-jobmanager
echo "Waiting to start up JobManager..."
sleep 15

docker compose --env-file .env up -d \
  --scale flink-taskmanager=$NUM_TASK_MANAGERS flink-taskmanager

sleep 10

docker compose --env-file .env up -d producer