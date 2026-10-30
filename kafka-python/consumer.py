
import json

from kafka import KafkaConsumer


class ServerMetricsConsumer:

    def __init__(self, bootstrap_servers, topic, group_id):
        # kafka consumer permite consumir mensajes de kafka
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            # gorup id identifica al groupo que pertenece el consumidor
            group_id=group_id,
            auto_offset_reset="earliest",
            value_deserializer=lambda value: json.loads(value.decode("utf-8"))
        )

    def process_event(self, event):
        metric_name = event["metric_name"]
        value = event["value"]
        server_id = event["server_id"]

        print(
            f"Server: {server_id} | "
            f"Metric: {metric_name} | "
            f"Value: {value}"
        )
# fijate que las condiciones van en el archivo
        if value > 80:
            print(
                f"WARNING: High {metric_name} usage "
                f"on {server_id}: {value}%"
            )

    def run(self):
        print("Consumer listening for server metrics...")

        try:
            for message in self.consumer:
                self.process_event(message.value)

        except KeyboardInterrupt:
            print("\nConsumer stopped.")

        finally:
            self.consumer.close()


if __name__ == "__main__":
    consumer = ServerMetricsConsumer(
        bootstrap_servers="localhost:9092",
        topic="server_metrics",
        group_id="server_monitoring_group"
    )

    consumer.run()
