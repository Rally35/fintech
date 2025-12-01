# Project Structure

This document outlines the structure of the project and provides a brief description of each file.

## Root Directory

-   `README.md`: Main readme file for the project.
-   `docker-compose.yml`: Defines the services, networks, and volumes for the Dockerized application. It sets up a Streamlit container and a PostgreSQL container, and defines their relationship.
-   `data/`: Contains data files used by the application.
-   `database/`: Contains database-related scripts.
-   `scripts/`: Contains standalone scripts.
-   `systemd/`: Contains systemd service and timer files for running scheduled tasks.
-   `tests/`: Contains tests for the application.
-   `app/`: Contains the main application code.

## `app` Directory

-   `Dockerfile`: Dockerfile for the main application container. It sets up the Python environment, installs dependencies, and defines the entrypoint.
-   `app.py`: The main Streamlit application file. It defines the user interface, handles user interactions, and displays the financial data and metrics.
-   `entrypoint.sh`: The entrypoint script for the Docker container. It waits for the database to be ready, runs data import scripts, and then starts the Streamlit application.
-   `requirements.txt`: A list of Python dependencies for the application.
-   `scripts/`: Contains scripts used by the application.
-   `utils/`: Contains utility functions.

### `app/scripts` Directory

-   `import_quarterly.py`: Script to import quarterly financial data from an Excel file into the database.
-   `update_prices.py`: Script to update stock prices by fetching data from Yahoo! Finance.

### `app/utils` Directory

-   `__init__.py`: Makes the `utils` directory a Python package.
-   `cache.py`: Caching utilities.
-   `db.py`: Database connection and query utilities. It includes a connection pool and functions to fetch data from the database.
-   `excel_import.py`: Utilities for importing data from Excel files. It includes functions to load, validate, and prepare data for database insertion.
-   `logger.py`: Logging configuration for the application.
-   `metrics.py`: Functions to calculate financial metrics like P/E ratio, ROE, etc.

## `data` Directory

-   `dane_finansowe.xlsx`: Excel file with financial data for various companies.

## `database` Directory

-   `init.sql`: SQL script to initialize the database schema. It creates the `companies`, `financials`, and `prices_daily` tables.
-   `seed_companies.sql`: SQL script to seed the `companies` table with some initial data.

## `scripts` Directory

-   `import_quarterly.py`: Standalone script to import quarterly financial data. This seems to be a duplicate of the script in `app/scripts`.
-   `update_prices.py`: Standalone script to update stock prices. This also seems to be a duplicate of the script in `app/scripts`.

## `systemd` Directory

-   `update-prices.service`: systemd service file to run the price update script as a service.
-   `update-prices.timer`: systemd timer file to schedule the `update-prices.service` to run on a daily basis.

## `tests` Directory

-   `test_metrics.py`: Tests for the financial metrics calculations in `app/utils/metrics.py`.
