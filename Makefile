# Makefile

.PHONY: up down build

start:
	docker run -d \
		--name nba_extractor \
		--network airflow_default \
		--env-file .env \
		nba_extractor_package:latest

down:
	docker stop nba_extractor
	docker rm nba_extractor
	docker image rm nba_extractor_package

build:
	docker build -t nba_extractor_package:latest .

up: build start