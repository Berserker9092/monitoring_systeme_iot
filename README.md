![alt text](<Capture d'écran 2026-11-24 141423.png>)


# IoT Maintenance Pipeline

Pipeline de streaming qui simule des capteurs industriels et surveille l'état
des machines en temps réel.

```text
Producer Python ──▶ Kafka (KRaft) ──▶ Spark Structured Streaming ──▶ Elasticsearch ──▶ Kibana
 4 machines          topic            parsing + règles d'alerte        index            dashboards
 simulées            iot-sensor-data  (statut OK / WARNING / CRITICAL)
```

Le producer envoie uniquement des mesures brutes (température, vibration,
courant, batterie, signal…). C'est Spark qui calcule les alertes et le statut
de chaque mesure avant l'écriture dans Elasticsearch. La liste des champs et
les seuils sont dans [docs/data-schema.md](docs/data-schema.md).

## Stack

- Kafka 3.6 (image Confluent Platform 7.6.1) en mode KRaft, sans ZooKeeper
- Spark 3.5 Structured Streaming (PySpark)
- Elasticsearch et Kibana 8.15
- Python 3.11 et `kafka-python`
- Docker Compose

## Lancer le projet

Prérequis : Docker avec Compose, et environ 8 Go de RAM.

```bash
git clone <url-du-depot>
cd iot-maintenance-pipeline
docker compose up --build
```

Le premier démarrage est long : Spark télécharge les connecteurs Kafka et
Elasticsearch au lancement du conteneur.

| Service       | URL                   |
|---------------|-----------------------|
| Kibana        | http://localhost:5601 |
| Elasticsearch | http://localhost:9200 |
| Kafka (hôte)  | localhost:9092        |

Arrêter : `docker compose down` (ajouter `-v` pour supprimer aussi les
volumes : données Elasticsearch, Kafka et checkpoints).

## Vérifier que ça tourne

```bash
docker compose ps
docker logs -f iot-producer        # une ligne par machine toutes les 2 s
docker logs -f spark-processor     # "Batch N écrit dans ..."
curl -s "http://localhost:9200/iot-telemetry-metrics/_count?pretty"
```

Dans Kibana : Stack Management → Data Views → créer une vue sur
`iot-telemetry-metrics*` avec `timestamp` comme champ de temps, puis
Discover. Le champ `status` permet de filtrer les mesures en alerte.

## Lancer les scripts sans Docker

Avec Kafka et Elasticsearch déjà démarrés (`docker compose up kafka elasticsearch`) :

```bash
pip install -r requirements.txt

# Terminal 1
KAFKA_BROKER=localhost:9092 python src/producers/iot_sensor_producer.py

# Terminal 2 (Java et Spark requis)
KAFKA_BROKER=localhost:9092 ES_NODES=localhost STARTUP_DELAY_SEC=0 \
  spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.elasticsearch:elasticsearch-spark-30_2.12:8.11.0 \
  src/processors/spark_streaming_processor.py
```

## Tests

```bash
pip install -r requirements.txt -r requirements-dev.txt
pytest
```

Les tests couvrent la génération des messages et les règles d'alerte Spark
(le test Spark lance une session locale, Java est donc nécessaire).

## Structure

```text
.
├── docker-compose.yml
├── docker/
│   ├── producer/Dockerfile
│   └── spark/Dockerfile
├── src/
│   ├── producers/iot_sensor_producer.py
│   └── processors/spark_streaming_processor.py
├── tests/
├── docs/data-schema.md
├── requirements.txt            # exécution locale
├── requirements-producer.txt   # image du producer
└── .env.example
```

## Limites

- Les alertes reposent sur des seuils fixes : il n'y a pas de modèle de
  prédiction. Les données sont simulées (valeur nominale + bruit + pics rares).
- Un seul broker Kafka et un seul nœud Elasticsearch, sans sécurité activée :
  la configuration convient pour une démo locale, pas pour de la production.
- Pas d'agrégation par fenêtre de temps pour l'instant (moyenne par minute et
  par machine, par exemple).

