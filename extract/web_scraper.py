from seleniumwire import webdriver
from seleniumwire.utils import decode
import json

def format_trails(records):
    top_trails = []
    num_records = 5
    
    for record in records[:num_records]:
        top_trails.append({
            'name': record['name'],
            'distance': float(record['distance'].replace(",", ".")),
            'slope': int(record['slope']),
            'lon': record['lon'],
            'lat': record['lat']
        })

    return top_trails


def fetch_trails():
    top_trails = []

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    driver = webdriver.Chrome(options=options)
    target_url = 'https://ca.wikiloc.com/wikiloc/map.do?sw=-55.983%2C-109.449&ne=-17.4978%2C-66.4164&place=Xile&page=1'
    # target_url = 'https://ca.wikiloc.com/wikiloc/map.do?sw=40.5231%2C0.1592&ne=42.8615%2C3.3223&place=Catalunya&page=1'

    driver.get(target_url)

    for request in driver.requests:
        if request.url.startswith('https://ca.wikiloc.com/wikiloc/find.do?event=map'):
            try:
                data = decode(request.response.body, request.response.headers.get('Content-Encoding', 'identity'))
                resp = json.loads(data.decode('utf-8'))
                top_trails = format_trails(resp['spas'])
                break
            except:
                pass
    
    driver.close()
    return top_trails