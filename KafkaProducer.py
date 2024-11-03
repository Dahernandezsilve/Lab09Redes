from confluent_kafka import Producer
import json
import random
import time
from typing import Dict

class KafkaProducer:
    def __init__(self, topic: str = "21270") -> None:
        self.bootstrapServers = '164.92.76.15:9092'
        self.topic = topic
        self.producer = Producer({
            'bootstrap.servers': self.bootstrapServers,
            'security.protocol': 'PLAINTEXT',
            'compression.type': 'gzip',
            'retries': 5,
            'linger.ms': 10
        })
        print("🚀 KafkaProducer initialized")

    def sendMessage(self, message: Dict) -> None:
        def deliveryReport(err, msg):
            if err:
                print(f"❌ Delivery failed: {err}")
            else:
                print(f"✅ Delivered to {msg.topic()} [{msg.partition()}]")

        encodedMessage = json.dumps(message).encode('utf-8')
        self.producer.produce(self.topic, key="sensor1", value=encodedMessage, callback=deliveryReport)
        self.producer.flush()
        print("📤 Message sent to Kafka")
        self.printStatus(message)

    def generateSensorData(self) -> Dict:
        temperature = round(random.gauss(55.0, 15.0), 2)
        temperature = max(0, min(temperature, 110))
        
        humidity = int(random.gauss(50, 20))
        humidity = max(0, min(humidity, 100))
        
        windDirections = ["N", "NO", "O", "SO", "S", "SE", "E", "NE"]
        windDirection = random.choice(windDirections)
        
        data = {
            "temperatura": temperature,
            "humedad": humidity,
            "direccion_viento": windDirection
        }
        print(f"🌡️  Generated sensor data: {data}")
        return data

    def printStatus(self, message: Dict) -> None:
        print("\n📊 Current Producer Status")
        print("════════════════════════════════")
        print(f"📍 Topic: {self.topic}")
        print(f"🔑 Key: sensor1")
        print(f"📦 Message: {json.dumps(message, indent=4)}")
        print("════════════════════════════════\n")

    def startProducing(self) -> None:
        try:
            while True:
                sensorData = self.generateSensorData()
                self.sendMessage(sensorData)
                
                interval = random.randint(15, 30)
                print(f"⏳ Waiting {interval} seconds before next data generation")
                time.sleep(interval)
        except KeyboardInterrupt:
            print("🛑 Stopping sensor data production.")
            self.producer.flush()
            print("🔒 Producer closed")

# Usage
if __name__ == "__main__":
    kafkaProducer = KafkaProducer()
    kafkaProducer.startProducing()
