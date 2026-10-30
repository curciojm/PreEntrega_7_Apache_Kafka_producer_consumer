
import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer


class ServerMetricsProducer:

    def __init__(self, bootstrap_servers):
        # KafkaProducer comunica la app de python con el broker de kafka
        # Usamos kafkaproducer dentro de la clase
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            # lambda asume clase sin nombre
            value_serializer=lambda value: json.dumps(value).encode("utf-8")
        )

    def generate_event(self):
        metric_name = random.choice(["cpu", "memory"])

        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "server_id": f"server_{random.randint(1, 3):02d}",
            "metric_name": metric_name,
            "value": round(random.uniform(20, 100), 2)
        }

    def send_event(self):
        event = self.generate_event()

        future = self.producer.send(
            "server_metrics",
            value=event
        )

        metadata = future.get(timeout=10)

        print(
            f"Event sent | "
            f"Partition: {metadata.partition} | "
            f"Offset: {metadata.offset} | "
            f"Data: {event}"
        )

    def run(self):
        try:
            while True:
                self.send_event()
                time.sleep(2)

        except KeyboardInterrupt:
            print("\nProducer stopped.")

        finally:
            # flush y close son de kafka python. flush borra los mensajes pendientes en el buffer una vez que se ejecutaron
            # close: cierra el productor y libera los recursos asociados
            self.producer.flush()
            self.producer.close()


if __name__ == "__main__":
    producer = ServerMetricsProducer(
        bootstrap_servers="localhost:9092"
    )

    producer.run()
