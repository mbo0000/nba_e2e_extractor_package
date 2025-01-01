# NBA Data Extractor
![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.x-green)
![Snowflake](https://img.shields.io/badge/Snowflake-%23f3f1ff)![Docker](https://img.shields.io/badge/Docker-%2B-blue)

## Introduction

Welcome to the NBA data extraction repository. This project will cover the Extraction and Load parts in the ELT data pipeline. It is an essential utility that enables seamless NBA games and statistics data extraction via a NBA package and uploads to Snowflake programmatically.

## High Level Design

1. **Data Extraction**: Retrieves data from specified endpoints.
2. **Data Formatting**: Prepare the data for insertion into Snowflake.
3. **Upload to Snowflake**: dump the raw data into staging area.

## Code Design Patterns

All entities in the NBA API follow a consistent pattern for data extraction, formatting, and uploading. Each entity, such as games or teams statistics, implements a standard set of methods and interfaces. When [main.py](https://github.com/mbo0000/nba_e2e_extractor_package/blob/main/main.py) is called externally, via Airflow, it determines which extractor subclass to use based on the provided arguments. During runtime, the behavior of each stage is encapsulated within individual subclasses.

## Installation

### Prerequisites

- Python 3.10+
- Snowflake Account
    - Follow Snowflake config and management instruction [here](https://github.com/mbo0000/nba_e2e_data_pipeline?tab=readme-ov-file#1-snowflake-management-and-config)
- Apache Airflow (2.x recommended)
- Docker
- [NBA API](https://github.com/swar/nba_api/tree/master) package

### Setup Steps

1. Clone the repository in your project directory:
    ```sh
    git clone https://github.com/mbo0000/nba_e2e_extractor_package
    cd nba_e2e_extractor_package

2. Create a `.env` file and provide credentials for Snowflake connection. Example: 
    ```
    SNOWF_USER=foo
    SNOWF_PW=foo
    SNOWF_ACCOUNT=foo
    SNOWF_ROLE=PC_USER
    SNOWF_TARGET_WH=LOADING
    ```
    Replace 'foo' with your actual creds. 

3. Build + start a Docker container:
    ```
    make start
    ```

## Usage
1. Manual execution via Docker, using games entity as example:
    ```
    docker exec -t extractor_package-app-1 /bin/bash
    python main.py --entity games --database staging --schema nba_dump --table games
    ```
2. tear down container:
    ```
    make down
    ```