IMAGE_NAME="create-kafka-topics"
docker build -t "$IMAGE_NAME" .
docker compose up --force-recreate -d