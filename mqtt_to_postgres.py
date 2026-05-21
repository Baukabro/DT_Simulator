import json
import psycopg2
import paho.mqtt.client as mqtt

# -----------------------------------
# PostgreSQL connection
# -----------------------------------
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="digital_twin",
    user="postgres",
    password="postgre"
)
conn.autocommit = True

cur = conn.cursor()

create_table_query = """
CREATE TABLE IF NOT EXISTS bus_telemetry (
    id SERIAL PRIMARY KEY,
    bus_id VARCHAR(50) NOT NULL,
    route_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    speed DOUBLE PRECISION NOT NULL,
    passenger_count INTEGER NOT NULL,
    door_status VARCHAR(10) NOT NULL,
    traffic_level VARCHAR(10) NOT NULL,
    event_type VARCHAR(20),
    event_duration INTEGER,
    current_stop_name VARCHAR(255),
    boarding_count INTEGER,
    alighting_count INTEGER,
    waiting_passengers INTEGER,
    dwell_remaining INTEGER
);
"""

cur.execute(create_table_query)

print("Table bus_telemetry is ready.")
alter_queries = [
    "ALTER TABLE bus_telemetry ADD COLUMN IF NOT EXISTS event_type VARCHAR(20);",
    "ALTER TABLE bus_telemetry ADD COLUMN IF NOT EXISTS event_duration INTEGER;",
    "ALTER TABLE bus_telemetry ADD COLUMN IF NOT EXISTS current_stop_name VARCHAR(255);",
    "ALTER TABLE bus_telemetry ADD COLUMN IF NOT EXISTS boarding_count INTEGER;",
    "ALTER TABLE bus_telemetry ADD COLUMN IF NOT EXISTS alighting_count INTEGER;",
    "ALTER TABLE bus_telemetry ADD COLUMN IF NOT EXISTS waiting_passengers INTEGER;",
    "ALTER TABLE bus_telemetry ADD COLUMN IF NOT EXISTS dwell_remaining INTEGER;"
]

for query in alter_queries:
    cur.execute(query)

print("Table bus_telemetry is ready and schema is updated.")


def on_connect(client, userdata, flags, rc):
    print("Connected to MQTT broker with rc =", rc)
    client.subscribe("bus/telemetry")
    print("Subscribed to topic: bus/telemetry")


def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        print("Received:", payload)

        insert_query = """
        INSERT INTO bus_telemetry (
            bus_id,
            route_id,
            timestamp,
            latitude,
            longitude,
            speed,
            passenger_count,
            door_status,
            traffic_level,
            event_type,
            event_duration,
            current_stop_name,
            boarding_count,
            alighting_count,
            waiting_passengers,
            dwell_remaining
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        data = (
            payload["busId"],
            payload["routeId"],
            payload["timestamp"],
            payload["latitude"],
            payload["longitude"],
            payload["speed"],
            payload["passengerCount"],
            payload["doorStatus"],
            payload["trafficLevel"],
            payload.get("eventType"),
            payload.get("eventDuration"),
            payload.get("currentStopName"),
            payload.get("boardingCount", 0),
            payload.get("alightingCount", 0),
            payload.get("waitingPassengers", 0),
            payload.get("dwellRemaining", 0)
        )

        cur.execute(insert_query, data)
        print("Inserted into PostgreSQL")

    except json.JSONDecodeError as e:
        print("JSON decode error:", e)
    except KeyError as e:
        print("Missing field in payload:", e)
    except Exception as e:
        print("Database insert error:", e)



client = mqtt.Client()

client.on_connect = on_connect
client.on_message = on_message

client.connect("localhost", 1883, 60)

print("MQTT subscriber is running...")
client.loop_forever()