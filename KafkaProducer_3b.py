from confluent_kafka import Producer
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

    def encode_message(self, temperature: float, humidity: int, wind_direction: str) -> bytes:
        # Mapear la dirección del viento a un valor de 3 bits
        wind_direction_mapping = {
            "N": 0,
            "NE": 1,
            "E": 2,
            "SE": 3,
            "S": 4,
            "SO": 5,
            "O": 6,
            "NO": 7
        }
        wind_dir_bits = wind_direction_mapping[wind_direction] & 0b111  # 3 bits

        # Convertir temperatura a un entero entre 0 y 11000
        temp_int = int(round(temperature * 100)) & 0x3FFF  # 14 bits

        # Humedad en 7 bits
        humidity_bits = humidity & 0x7F  # 7 bits

        # Construir el mensaje de 24 bits
        message_bits = (temp_int << 10) | (humidity_bits << 3) | wind_dir_bits

        # Convertir a bytes
        message_bytes = message_bits.to_bytes(3, byteorder='big')

        return message_bytes

    def sendMessage(self, message: Dict) -> None:
        def deliveryReport(err, msg):
            if err:
                print(f"❌ Delivery failed: {err}")
            else:
                print(f"✅ Delivered to {msg.topic()} [{msg.partition()}]")

        encodedMessage = self.encode_message(
            message["temperatura"],
            message["humedad"],
            message["direccion_viento"]
        )
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
        print(f"📦 Message: {message}")
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
