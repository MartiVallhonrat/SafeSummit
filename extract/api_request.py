from dotenv import load_dotenv
import os
import requests
from datetime import datetime, timezone

def format_weather(record):
    coord = record['coord']
    main = record['main']
    visibility = record['visibility']
    wind = record['wind']
    if 'rain' in record:
        rain = record['rain']
    else:
        rain = {'1h': 0}
    if 'snow' in record:
        snow = record['snow']
    else:
        snow = {'1h': 0}
    dt = datetime.fromtimestamp(record['dt'], tz=timezone.utc)
    sunrise = datetime.fromtimestamp(record['sys']['sunrise'], tz=timezone.utc)
    sunset = datetime.fromtimestamp(record['sys']['sunset'], tz=timezone.utc)

    fomated_record = {
        'lon': coord['lon'],
        'lat': coord['lat'], 
        'temp': main['temp'],
        'humidity': main['humidity'],
        'visibility': visibility,
        'wind': wind['speed'],
        'rain': rain['1h'],
        'snow': snow['1h'],
        'dt': dt,
        'sunrise': sunrise,
        'sunset': sunset,
    }

    return fomated_record

def fetch_weather(lat, lon):
    load_dotenv()

    api_key = os.getenv('OPENWEATHER_API_KEY')
    if not api_key:
        raise ValueError('Error: Missing "OPENWEATHER_API_KEY" enviroment variable')
    
    api_url = f'https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units=metric'

    print('Fetching weather data from the API...')
    try:
        resp = requests.get(api_url)
        resp.raise_for_status()
        record = resp.json()
        formated_record = format_weather(record)
        print('API response recived succesfully!')
        return formated_record
    except:
        print(f'An error ocurred during the OpenWeatherAPI petition')
        raise

# try:
#     print(fetch_weather(0, 0))
# except Exception as e:
#     print(f'{e}')
