from confluent_kafka import Consumer
import matplotlib.pyplot as plt
import time

class KafkaConsumer:
    def __init__(self, topic: str = "21270") -> None:
        self.bootstrapServers = '164.92.76.15:9092'
        self.topic = topic
        self.consumer = Consumer({
            'bootstrap.servers': self.bootstrapServers,
            'group.id': 'weather_station_consumer_group',
            'auto.offset.reset': 'earliest'
        })
        self.consumer.subscribe([self.topic])
        print("🌐 KafkaConsumer initialized and subscribed to topic:", self.topic)
        self.temperatures = []
        self.humidities = []
        self.wind_directions = []
        self.timestamps = []
        plt.style.use("dark_background")
        plt.ion() 
        self.fig, self.ax = plt.subplots(figsize=(12, 6))
        self.temp_line, = self.ax.plot([], [], 'r-o', label="Temperature (°C)")
        self.humidity_line, = self.ax.plot([], [], 'b-x', label="Humidity (%)")
        self.ax.set_xlabel("Timestamp")
        self.ax.set_ylabel("Values")
        self.ax.set_title("Telemetry Data")
        self.ax.legend(loc="upper left")

    def decode_message(self, message_bytes: bytes) -> dict:
        # Convertir bytes a un entero de 24 bits
        message_bits = int.from_bytes(message_bytes, byteorder='big')

        wind_dir_bits = message_bits & 0b111  # Últimos 3 bits
        humidity_bits = (message_bits >> 3) & 0x7F  # Siguientes 7 bits
        temp_int = (message_bits >> 10) & 0x3FFF  # Primeros 14 bits

        # Convertir de vuelta a valores originales
        temperature = temp_int / 100.0
        humidity = humidity_bits
        wind_direction_mapping = {
            0: "N",
            1: "NE",
            2: "E",
            3: "SE",
            4: "S",
            5: "SO",
            6: "O",
            7: "NO"
        }
        wind_direction = wind_direction_mapping[wind_dir_bits]

        data = {
            "temperatura": temperature,
            "humedad": humidity,
            "direccion_viento": wind_direction
        }
        return data

    def consumeMessages(self) -> None:
        try:
            while True:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    print("❌ Consumer error:", msg.error())
                    continue

                data = self.decode_message(msg.value())
                self.updateData(data)
                
                print("📥 Message received:", data)
                self.printStatus(data)
                self.updatePlot()
                
                time.sleep(1)
        except KeyboardInterrupt:
            print("🛑 Stopping consumer.")
        finally:
            self.consumer.close()
            print("🔒 Consumer closed")

    def updateData(self, data: dict) -> None:
        self.temperatures.append(data["temperatura"])
        self.humidities.append(data["humedad"])
        self.wind_directions.append(data["direccion_viento"])
        self.timestamps.append(time.strftime("%H:%M:%S"))
        
        if len(self.temperatures) > 8:
            self.temperatures.pop(0)
            self.humidities.pop(0)
            self.wind_directions.pop(0)
            self.timestamps.pop(0)

    def printStatus(self, data: dict) -> None:
        print("\n📊 Current Consumer Status")
        print("════════════════════════════════")
        print(f"🌡️ Temperature: {data['temperatura']} °C")
        print(f"💧 Humidity: {data['humedad']} %")
        print(f"🧭 Wind Direction: {data['direccion_viento']}")
        print("════════════════════════════════\n")

    def updatePlot(self) -> None:
        self.temp_line.set_data(self.timestamps, self.temperatures)
        self.humidity_line.set_data(self.timestamps, self.humidities)
        
        [child.remove() for child in self.ax.get_children() if isinstance(child, plt.Annotation)]
        
        for i, (temp, hum, direction) in enumerate(zip(self.temperatures, self.humidities, self.wind_directions)):
            x = self.timestamps[i]
            y = max(temp, hum) + 5
            annotation_text = f"Wind direction: {direction}\nTemp: {temp}°C\nHumidity: {hum}%"
            self.ax.annotate(annotation_text, (x, y), color="white", fontsize=8, ha="center", va="bottom",
                             bbox=dict(facecolor="black", edgecolor="white", boxstyle="round,pad=0.3"))

        self.ax.set_xlim(self.timestamps[0], self.timestamps[-1])
        self.ax.set_ylim(0, max(max(self.temperatures), max(self.humidities)) + 20)

        self.fig.autofmt_xdate(rotation=45)
        self.fig.canvas.draw()
        self.fig.canvas.flush_events()

if __name__ == "__main__":
    kafkaConsumer = KafkaConsumer()
    kafkaConsumer.consumeMessages()
