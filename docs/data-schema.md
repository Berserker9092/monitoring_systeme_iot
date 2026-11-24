# Schéma des données

Un message = une mesure d'une machine, publiée toutes les `SEND_INTERVAL_SEC`
secondes pour chacune des 4 machines.

## Envoyé par le producer (mesures brutes)

| Champ                 | Type   | Description                                   |
|-----------------------|--------|-----------------------------------------------|
| `timestamp`           | string | Date ISO 8601 UTC de la mesure                |
| `event_time`          | long   | Même instant, en millisecondes epoch          |
| `machine_id`          | string | Identifiant machine (`MCH-xxx`)               |
| `plant_location`      | string | Site (`Plant_Dakar_1`, `Plant_Thies_2`, …)    |
| `temperature_celsius` | double | Température                                   |
| `humidity_percent`    | double | Humidité                                      |
| `vibration_hz`        | double | Vibration                                     |
| `pressure_bar`        | double | Pression                                      |
| `courant_ampere`      | double | Courant                                       |
| `tension_volt`        | double | Tension                                       |
| `puissance_kw`        | double | Puissance instantanée (courant × tension)     |
| `consommation_kwh`    | double | Énergie consommée sur l'intervalle            |
| `battery_percent`     | double | Niveau de batterie du boîtier                 |
| `rssi_dbm`            | int    | Qualité du signal radio                       |
| `uptime_seconds`      | int    | Durée de fonctionnement du boîtier            |
| `firmware_version`    | string | Version du firmware                           |
| `message_seq`         | long   | Numéro de séquence (détection de pertes)      |
| `last_seen`           | string | Dernière communication                        |

## Ajouté par Spark (règles d'alerte)

| Champ                   | Règle                                        |
|-------------------------|----------------------------------------------|
| `is_temp_critical`      | `temperature_celsius > 80`                   |
| `is_vibration_critical` | `vibration_hz > 4.5`                         |
| `is_courant_critical`   | `courant_ampere > 35`                        |
| `is_battery_low`        | `battery_percent < 20`                       |
| `is_signal_weak`        | `rssi_dbm < -80`                             |
| `status`                | voir ci-dessous                              |

`status` vaut :

- `CRITICAL` si l'un des trois drapeaux `*_critical` est vrai ;
- sinon `WARNING` si température > 65, vibration > 3, courant > 28 ou batterie faible ;
- sinon `OK`.

Les seuils sont des constantes en tête de `src/processors/spark_streaming_processor.py`.
