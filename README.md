# weather-data-analysis
A Python project for collecting, processing, and visualizing real-time weather data using PostgreSQL and open-meteo API

# Weather Data Collector

## Overview
This project collects, processes, and stores real-time weather data using the Open-Meteo API. The data is stored in a PostgreSQL database, and we visualize it with Python using Pandas and Matplotlib.

## Features
- Fetches current weather data (temperature, wind direction, wind speed) based on latitude and longitude.
- Stores weather data in a PostgreSQL database.
- Automatically schedules data collection using APScheduler.
- Visualizes historical weather data using Matplotlib.
- Implements basic data validation to ensure data integrity.

## Technologies Used
- Python
- PostgreSQL
- SQLAlchemy
- APScheduler
- Pandas
- Matplotlib
- OpenWeatherMap API

## Installation

1. Clone the repository:
    ```bash
    git clone https://github.com/yourusername/weather-data-collector.git
    ```

2. Install the required Python libraries:
    ```bash
    pip install -r requirements.txt
    ```

3. Set up PostgreSQL:
    - Create a database in PostgreSQL.
    - Update the database credentials in the `DATABASE` configuration in the Python script.

4. Run the script:
    ```bash
    python weather_data.py
    ```

## Usage

- Edit the latitude and longitude for the location you want to collect weather data for.
- The weather data will be collected and saved in the PostgreSQL database at regular intervals (default: every 5 minutes).
- You can also visualize the aggregated data using the `plot_weather_data()` function.

## Future Improvements
- Add more advanced data validation.
- Integrate more visualization tools.
- Expand the API to include more weather parameters.
