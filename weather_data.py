#!pip install pandas apscheduler requests psycopg2-binary sqlalchemy
import requests
import pandas as pd
import psycopg2
import matplotlib.pyplot as plt
from sqlalchemy import create_engine,Table, Column, Integer, String, Float, MetaData,inspect
from apscheduler.schedulers.background import BackgroundScheduler
import time
from sqlalchemy.engine.url import URL
import logging


##### CREATE AND SET UP DATABSE #####
# Create the SQLAlchemy engine
DATABASE = {
    'drivername': 'postgresql+psycopg2',
    'host': 'c3l5o0rb2a6o4l.cluster-czz5s0kz4scl.eu-west-1.rds.amazonaws.com',
    'port': '5432',
    'username': 'u5qvldnn7irbeu',  # PostgreSQL username
    'password': 'pd7eff6db3bd8176f5403b982caf818bf51129d98cdfb9bc2f77604a9db017554',  # PostgreSQL password
    'database': 'd46c3ieijibcp5'  # PostgreSQL database name
}

DATABASE_URL = f"postgresql+psycopg2://{DATABASE['username']}:{DATABASE['password']}@{DATABASE['host']}:{DATABASE['port']}/{DATABASE['database']}"
engine = create_engine(DATABASE_URL, pool_size=10, max_overflow=20,pool_recycle=3600)

#check all DB tables
# Create an inspector object to inspect the database
inspector = inspect(engine)

# Get a list of all table names
table_names = inspector.get_table_names()

# Print the table names
#print("Tables in the database:")
#for table_name in table_names:
#    print(table_name)
# Define metadata and table schema
metadata = MetaData()

weather_data_table = Table('weather_data', metadata,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('city', String(50)),
    Column('timestamp', String(50)),
    Column('temperature', Float),
    Column('wind_direction', Float),
    Column('wind_speed', Float),
)

weather_data_table = Table('weather_data', metadata, autoload_with=engine)


if 'timestamp' not in weather_data_table.columns:
    with engine.connect() as connection:
        # Add the 'timestamp' column
        connection.execute('ALTER TABLE weather_data ADD COLUMN timestamp VARCHAR(50);')
        #print("Added 'timestamp' column to 'weather_data' table.")

# Create the table in the database
metadata.create_all(engine)

# Print the data of a table
query = "SELECT * FROM weather_data"
df = pd.read_sql(query, engine)
#print("Rows in the 'weather_data' table:")
print(df)


####################################################
logging.basicConfig(filename='app.log', level=logging.INFO)

##### FUNCTIONS #####



def fetch_weather_data(latitude, longitude):
    url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true"
    try:
        response = requests.get(url)
        response.raise_for_status()
        logging.info(f"Fetched data successfully for {latitude}, {longitude}")
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Error fetching weather data: {e}")
        return None

def get_city_name(latitude, longitude):
    url = f"https://nominatim.openstreetmap.org/reverse?format=json&lat={latitude}&lon={longitude}&zoom=10&addressdetails=1"
    headers = {
        'User-Agent': 'cmoutzou/1.0 (your.email@example.com)'      }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        return data['address'].get('city', 'Unknown city')
    else:
        print(f"Failed to fetch city name: {response.status_code}")
        return None

def process_weather_data(weather_data, latitude, longitude):
    if weather_data:
        try:
            current_weather = weather_data.get('current_weather', {})

            # Debug: Print weather data structure
            #print("Weather Data Structure:", weather_data)

            # Access the 'time' from the 'current_weather' dictionary
            timestamp = current_weather.get('time', 'No Timestamp Provided')

            # Prepare the weather data
            weather = {
                'city': get_city_name(latitude, longitude),
                'timestamp': timestamp,
                'temperature': current_weather.get('temperature', None),
                'wind_direction': current_weather.get('winddirection', None),
                'wind_speed': current_weather.get('windspeed', None)
            }

            # Create a DataFrame
            df = pd.DataFrame([weather])

            # Debug: Print processed DataFrame
            #print("Processed DataFrame:")
            #print(df)
            #print("Columns:", df.columns)

            return df
        except Exception as e:
            print(f"Error processing weather data: {e}")
    return pd.DataFrame(columns=['city', 'timestamp', 'temperature', 'wind_direction', 'wind_speed'])

# Function to save data
def save_data_to_db(df, table_name='weather_data'):
    try:
        #print("Data to be saved to DB:")
        #print(df)
        with engine.connect() as connection:
            df.to_sql(table_name, connection, if_exists='append', index=False)
        #print(f"Data saved to the '{table_name}' table in the database.")
    except Exception as e:
        print(f"Error saving data to database: {e}")

#save_data_to_db(process_weather_data(weather_data,latitude, longitude))

def scheduled_job(latitude, longitude):
    try:
        weather_data = fetch_weather_data(latitude, longitude)
        if weather_data:
            processed_data = process_weather_data(weather_data, latitude, longitude)
            if not processed_data.empty:
                try:
                    with engine.connect() as connection:
                        processed_data.to_sql('weather_data', connection, if_exists='append', index=False)
                    print("Data saved to the database.")
                except Exception as e:
                    print(f"Error saving data to database: {e}")
                finally:
                    connection.close()
    except Exception as e:
        logging.error(f"Scheduled job failed: {e}")

def close_connections():
    engine.dispose()

#vALIDATION
def validate_weather_data(df):
    if df['temperature'].min() < -100 or df['temperature'].max() > 60:
        print("Warning: Temperature values are out of expected range.")

#Visualization
def aggregate_weather_data(df):
    if df.empty:
        return df
    # Convert 'timestamp' to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df = df.dropna(subset=['timestamp'])  # Drop rows with invalid timestamps
    # Set 'timestamp' as index
    df.set_index('timestamp', inplace=True)
    # Resample by hour and take the mean of numeric columns only
    df_resampled = df.resample('H').mean(numeric_only=True)
    return df_resampled.reset_index()


def plot_weather_data(df):
    # Check if required columns are present
    required_columns = ['timestamp', 'temperature']
    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        print(f"Error: Missing columns: {', '.join(missing_columns)}")
        return

    # Convert 'timestamp' to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')  # Convert to datetime
    df = df.dropna(subset=['timestamp'])  # Drop rows with invalid timestamps

    # Set 'timestamp' as index
    df = df.set_index('timestamp')
    plt.figure(figsize=(10, 5))
    plt.plot(df.index, df['temperature'], label='Temperature')
    plt.xlabel('Time')
    plt.ylabel('Temperature')
    plt.title(f'Weather Data Over Time for {get_city_name(latitude, longitude)}')
    plt.legend()
    plt.xticks(rotation=45)  # Rotate x-axis labels for better readability
    plt.tight_layout()  # Adjust layout to fit labels
    plt.show()




############################################

# Example usage:
close_connections()
latitude = 37.9838   # Latitude for Athens
longitude = 23.7275  # Longitude for Athens


weather_data = fetch_weather_data(latitude, longitude)
scheduler = BackgroundScheduler()
scheduler.add_job(scheduled_job, 'interval', minutes=1, args=[latitude, longitude])
scheduler.start()

#print data
print(get_city_name(latitude, longitude))
print(process_weather_data(weather_data,latitude, longitude))

validate_weather_data(process_weather_data(weather_data,latitude, longitude))

if weather_data:
    print(weather_data)

scheduler = BackgroundScheduler()
scheduler.add_job(scheduled_job, 'interval', minutes=5, args=[latitude, longitude])  # Run every 30 minutes
scheduler.start()

all_data_df = pd.read_sql(f"SELECT * FROM weather_data WHERE city='{get_city_name(latitude, longitude)}'", engine)
#print("Data fetched from DB:")
#print(all_data_df)

# Plot data
all_data_df = pd.read_sql(f"SELECT * FROM weather_data WHERE city='{get_city_name(latitude, longitude)}'", engine)
aggregated_data = aggregate_weather_data(all_data_df)
plot_weather_data(aggregated_data)


try:
    while True:
        time.sleep(60)  # Sleep for 60 seconds to keep the script running
except (KeyboardInterrupt, SystemExit):
    print("Shutting down scheduler...")
    scheduler.shutdown(wait=True)
    close_connections()
