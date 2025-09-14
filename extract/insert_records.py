from dotenv import load_dotenv
import os
import psycopg2 as pg2

def connect_db():
    load_dotenv()

    HOST = 'postgres_db'
    PORT = 5432
    DB_NAME = os.getenv('POSTGRES_DB')
    if not DB_NAME:
        raise ValueError('Error: Missing "POSTGRES_DB" enviroment variable')
    USER = os.getenv('POSTGRES_USER')
    if not USER:
        raise ValueError('Error: Missing "POSTGRES_USER" enviroment variable')
    PASSWORD = os.getenv('POSTGRES_PASSWORD')
    if not PASSWORD:
        raise ValueError('Error: Missing "POSTGRES_PASSWORD" enviroment variable')


    print('Connecting to database...')
    try:
        conn = pg2.connect(
            host=HOST,
            port=PORT,
            dbname=DB_NAME,
            user=USER,
            password=PASSWORD
        )
        return conn
    except pg2.Error as e:
        print('Database connection failed!')
        raise

def create_trail_table(conn):
    print('Creating trail table...')
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS dev;
            CREATE TABLE IF NOT EXISTS dev.raw_trail_data (
                id SERIAL PRIMARY KEY,
                name TEXT,
                distance REAL,
                slope SMALLINT,
                lon REAL,
                lat REAL,
                inserted_at TIMESTAMP DEFAULT NOW()
            );
        """)
        conn.commit()
        print('Trail table created successfully!')
    except:
        print(f'Error creating trail table')
        raise

def create_weather_table(conn):
    print('Creating weather table...')
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS dev;
            CREATE TABLE IF NOT EXISTS dev.raw_weather_data (
                id SERIAL PRIMARY KEY,
                lon REAL,
                lat REAL,
                temperature REAL,
                humidity SMALLINT,
                visibility SMALLINT,
                wind REAL,
                rain REAL,
                snow REAL,
                datetime TIMESTAMPTZ,
                sunrise TIMESTAMPTZ,
                sunset TIMESTAMPTZ,
                inserted_at TIMESTAMP DEFAULT NOW()
            );
        """)
        conn.commit()
        print('Weather table created successfully!')
    except:
        print(f'Error creating weather table')
        raise

def insert_trail_batch(conn, data):
    try:
        print('Inserting trail batch...')
        cursor = conn.cursor()
        cursor.executemany("""
            INSERT INTO dev.raw_trail_data (
                name,
                distance,
                slope,
                lon,
                lat,
                inserted_at               
            ) VALUES (%s, %s, %s, %s, %s, NOW());
        """, data)
        conn.commit()
        print('Trail batch successfully inserted!')
    except:
        print(f'Error inserting trail batch')
        raise

def insert_weather_batch(conn, data):
    try:
        print('Inserting weather batch...')
        cursor = conn.cursor()
        cursor.executemany("""
            INSERT INTO dev.raw_weather_data (
                lon,
                lat,
                temperature,
                humidity,
                visibility,
                wind,
                rain,
                snow,
                datetime,
                sunrise,
                sunset,
                inserted_at              
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW());
        """, data)
        conn.commit()
        print('Weather batch successfully inserted!')
    except:
        print(f'Error inserting weather batch')
        raise

def insert_trail(trail_data):
    try:
        conn = connect_db()

        create_trail_table(conn)

        insert_trail_batch(conn, [(
            trail['name'],
            trail['distance'],
            trail['slope'],
            trail['lon'],
            trail['lat']
        ) for trail in trail_data])
    except Exception as e:
        print(e)
    finally:
        if 'conn' in locals():
            conn.close()
            print('Database connection closed')

def insert_weather(weather_data):
    try:
        conn = connect_db()

        create_weather_table(conn)

        insert_weather_batch(conn, [(
            weather['lon'],
            weather['lat'],
            weather['temp'],
            weather['humidity'],
            weather['visibility'],
            weather['wind'],
            weather['rain'],
            weather['snow'],
            weather['dt'],
            weather['sunrise'],
            weather['sunset']
        ) for weather in weather_data])
    except Exception as e:
        print(e)
    finally:
        if 'conn' in locals():
            conn.close()
            print('Database connection closed')  

# def main():
#     try:
#         trail_data = fetch_trails()
#         weather_data = [fetch_weather(trail['lat'], trail['lon']) for trail in trail_data]

#         conn = connect_db()
        
#         create_trail_table(conn)
#         create_weather_table(conn)

#         insert_trail_batch(conn, [(
#             trail['name'],
#             trail['distance'],
#             trail['slope'],
#             trail['lon'],
#             trail['lat']
#         ) for trail in trail_data])
#         insert_weather_batch(conn, [(
#             weather['lon'],
#             weather['lat'],
#             weather['temp'],
#             weather['humidity'],
#             weather['visibility'],
#             weather['wind'],
#             weather['rain'],
#             weather['snow'],
#             weather['dt'],
#             weather['sunrise'],
#             weather['sunset']
#         ) for weather in weather_data])
#     except Exception as e:
#         print(e)
#     finally:
#         if 'conn' in locals():
#             conn.close()
#             print('Database connection closed')
