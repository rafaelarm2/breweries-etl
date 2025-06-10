.PHONY: start_services stop_services test

start_services:
	mkdir -p data/dlh data/brewery-landing
	docker-compose up --build

stop_services:
	docker-compose down

test:
	poetry run pytest


# Usage:
#   start_services         # Start all services
#   stop_services          # Stop all services