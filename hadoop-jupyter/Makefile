.PHONY: start stop destroy pg-data

start:
	echo "Creating base container image"
	docker build . -t cluster-base
	echo "Running micro-cluster on background"
	docker network create cluster-network
	docker-compose up -d

stop:
	echo "Stopping micro-cluster"
	docker-compose down
	docker network rm cluster-network

destroy:
	echo "Stopping micro-cluster and removing volumes"
	docker-compose down -v

start-m1:
	echo "Creating base container image - Mac M1/M2"
	docker build . -t cluster-base --build-arg="ARCH=arm64"
	echo "Running micro-cluster on background"
	docker network create cluster-network || true
	docker-compose up -d

pg-data:
	echo "Starting pg-data only"
	docker-compose up -d pg-data