echo "DOCKER_HOST_IP=$(hostname -I | awk '{print $1}')" > .env
docker-compose -f docker-compose.yml up --build -d