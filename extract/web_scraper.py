from seleniumwire import webdriver
from seleniumwire.utils import decode
import json

def format_records(records):
    top_records = []
    num_records = 5
    
    for record in records[:num_records]:
        top_records.append({
            'name': record['name'],
            'distance': float(record['distance'].replace(",", ".")),
            'slope': int(record['slope']),
            'lon': record['lon'],
            'lat': record['lat']
        })

    return top_records


def fetch_records():
    top_records = []
    driver = webdriver.Firefox()
    target_url = 'https://ca.wikiloc.com/wikiloc/map.do?sw=40.5231%2C0.1592&ne=42.8615%2C3.3223&place=Catalunya&page=1'

    driver.get(target_url)

    for request in driver.requests:
        if request.url.startswith('https://ca.wikiloc.com/wikiloc/find.do?event=map'):
            try:
                data = decode(request.response.body, request.response.headers.get('Content-Encoding', 'identity'))
                resp = json.loads(data.decode('utf-8'))
                top_records = format_records(resp['spas'])
                break
            except:
                pass
    
    driver.close()
    return top_records