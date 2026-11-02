#!/usr/bin/env bash
# Starts the local emulators the adapter tests run against (no cloud account, no credentials), and prints the
# environment the tests read. Usage:
#   eval "$(scripts/test-env.sh up)"   # start them and export the variables
#   scripts/test-env.sh down           # remove them
set -euo pipefail
P=jobqueue-test
case "${1:-up}" in
up)
  run() { docker start "$P-$1" >/dev/null 2>&1 || docker run -d --name "$P-$1" "${@:2}" >/dev/null; }
  run nats -p 127.0.0.1:4231:4222 nats:2.15.2-alpine -js
  run sqs -p 127.0.0.1:9331:9324 softwaremill/elasticmq-native:1.6.14
  run dynamodb -p 127.0.0.1:8001:8000 amazon/dynamodb-local:2.6.1 -jar DynamoDBLocal.jar -inMemory
  run pubsub -p 127.0.0.1:8681:8681 gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators \
    gcloud beta emulators pubsub start --host-port=0.0.0.0:8681 --project=jobqueue-test
  run firestore -p 127.0.0.1:8682:8682 gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators \
    gcloud beta emulators firestore start --host-port=0.0.0.0:8682 --project=jobqueue-test
  run valkey -p 127.0.0.1:6393:6379 valkey/valkey:8.1-alpine
  # The cookie is given as an argument: some hosts refuse the file the image writes (eacces).
  run rabbitmq -p 127.0.0.1:5673:5672 -e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS="-setcookie jobqueuetest" \
    -e RABBITMQ_CTL_ERL_ARGS="-setcookie jobqueuetest" rabbitmq:4.1-alpine
  run kafka -p 127.0.0.1:19093:19093 --ulimit nofile=65536:65536 redpandadata/redpanda:v25.2.9 redpanda start --mode dev-container --smp 1 \
    --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19093 \
    --advertise-kafka-addr internal://localhost:9092,external://127.0.0.1:19093
  run postgres -p 127.0.0.1:5551:5432 -e POSTGRES_PASSWORD=test -e POSTGRES_DB=jobqueue postgres:16-alpine
  for i in $(seq 1 60); do
    curl -sf http://127.0.0.1:8681 >/dev/null 2>&1 && curl -sf http://127.0.0.1:8682 >/dev/null 2>&1 \
      && docker exec $P-postgres pg_isready -q -U postgres 2>/dev/null \
      && docker exec $P-rabbitmq rabbitmq-diagnostics -q check_port_connectivity >/dev/null 2>&1 \
      && docker exec $P-kafka rpk cluster health 2>/dev/null | grep -q 'Healthy:.*true' && break
    sleep 1
  done
  # A test makes many small topics: let Redpanda's small dev-container memory hold them.
  docker exec $P-kafka rpk cluster config set topic_memory_per_partition 16384 >/dev/null 2>&1 || true
  docker exec $P-kafka rpk cluster config set topic_fds_per_partition 1 >/dev/null 2>&1 || true
  cat <<VARS
export JOBQUEUE_TEST_NATS_URL=nats://127.0.0.1:4231
export JOBQUEUE_TEST_SQS_ENDPOINT=http://127.0.0.1:9331
export JOBQUEUE_TEST_DYNAMODB_ENDPOINT=http://127.0.0.1:8001
export JOBQUEUE_TEST_PUBSUB_EMULATOR=127.0.0.1:8681
export JOBQUEUE_TEST_FIRESTORE_EMULATOR=127.0.0.1:8682
export JOBQUEUE_TEST_REDIS_ADDR=127.0.0.1:6393
export JOBQUEUE_TEST_RABBITMQ_URL=amqp://guest:guest@127.0.0.1:5673/
export JOBQUEUE_TEST_KAFKA_BROKERS=127.0.0.1:19093
export JOBQUEUE_TEST_POSTGRES_URL=postgres://postgres:test@127.0.0.1:5551/jobqueue?sslmode=disable
VARS
  ;;
down)
  docker rm -f $P-nats $P-sqs $P-dynamodb $P-pubsub $P-firestore $P-valkey $P-rabbitmq $P-kafka $P-postgres >/dev/null 2>&1 || true
  ;;
esac
