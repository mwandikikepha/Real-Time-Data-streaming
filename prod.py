from kafka import KafkaProducer
from faker import  Faker
import requests
import time
import json

#fake = Faker()
cities = ['Nairobi', 'Mombasa', 'Kisumu', 'Nakuru','London','Eldoret','Dodoma','Kigali','Meru','Malindi']


API = "2002102ef9b4e3ec08e61af1497c3a41"

def weather_stream():
    payloads = []
    for city in cities:
        url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API}"
        response = requests.get(url)
        data =response.json()
        payload = {
        "city": city, 
        "temp": data["main"]["temp"],
        "humidity": data["main"]["humidity"],
        "condition": data["weather"][0]["main"],
        "timestamp": data["dt"]
    }
        payloads.append(payload)
    return payloads
    


producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'))

while True:
    weather_data = weather_stream()
    for weather in weather_data:
        producer.send('class', weather)
        print(f"Sent: {weather}")
        #time.sleep(0)
        

