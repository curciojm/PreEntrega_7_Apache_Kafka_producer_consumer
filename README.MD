# Apache Kafka – Server Metrics Streaming

## Descripción

Implementación de un pipeline de procesamiento de eventos en tiempo real utilizando Apache Kafka y Python. Se generan métricas sintéticas de CPU y memoria, que se publican en un topic y son procesadas por un consumidor independiente.

## Estructura

```text
.
├── apache-kafka/
│   ├── producer.py
│   └── consumer.py
├── docker-compose.yml
└── README.md
```

## Implementación

**1. Inicio de Kafka**

```bash
docker compose up -d
```

**2. Creación del topic**

```bash
docker exec -it kafka-server /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic server_metrics \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1
```

**3. Instalación de dependencias**

```bash
pip install kafka-python
```

**4. Ejecución del Consumer y Producer**

En terminales independientes:

```bash
python consumer/consumer.py
```

```bash
python producer/producer.py
```

## Verificación

Se verificó la publicación y el consumo de eventos, así como la distribución en las tres particiones del topic.

El Consumer procesa las métricas recibidas y genera una advertencia (`WARNING`) cuando el valor de CPU o memoria supera el 80%.