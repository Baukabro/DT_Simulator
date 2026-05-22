from pathlib import Path
from urllib.request import urlopen
from urllib.parse import quote
import json

import pandas as pd
from fastapi import Body, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import psycopg2


BASE_DIR = Path(__file__).resolve().parent
ROUTE_DIR = BASE_DIR / "data" / "routes" / "route_57"
STOPS_FILE = ROUTE_DIR / "route_57_stops.csv"
SCENARIO_CONTROL_FILE = BASE_DIR / "data" / "runtime" / "route57_scenario_control.json"

ALLOWED_ROUTE57_SCENARIOS = {
    "NONE",
    "R57_ROADWORKS_SARAISHYK",
    "R57_TRAFFIC_JAM_SARAISHYK",
    "R57_SECURITY_CLOSURE_MINISTRY"
}

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="digital_twin",
        user="postgres",
        password="postgre"
    )


def read_csv_auto(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    try:
        df = pd.read_csv(path, sep=";", encoding="utf-8-sig")
        if len(df.columns) > 1:
            return df
    except Exception:
        pass

    try:
        df = pd.read_csv(path, sep=",", encoding="utf-8-sig")
        if len(df.columns) > 1:
            return df
    except Exception:
        pass

    return pd.read_csv(path, sep=None, engine="python", encoding="utf-8-sig")


def load_route_stops():
    df = read_csv_auto(STOPS_FILE)
    df.columns = df.columns.str.strip().str.lower()

    required_columns = {"stop_sequence", "stop_name", "latitude", "longitude"}
    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"route_57_stops.csv missing columns: {missing_columns}. "
            f"Current columns: {df.columns.tolist()}"
        )

    if "demand_level" not in df.columns:
        df["demand_level"] = "MEDIUM"

    df["stop_sequence"] = pd.to_numeric(df["stop_sequence"], errors="coerce")
    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
    df["demand_level"] = df["demand_level"].fillna("MEDIUM").astype(str).str.upper()

    df = df.dropna(subset=["stop_sequence", "stop_name", "latitude", "longitude"])
    df = df.sort_values("stop_sequence").reset_index(drop=True)

    stops = []

    for index, row in df.iterrows():
        stops.append({
            "id": int(row["stop_sequence"]),
            "index": int(index),
            "name": str(row["stop_name"]),
            "latitude": float(row["latitude"]),
            "longitude": float(row["longitude"]),
            "demandLevel": str(row["demand_level"])
        })

    return stops


def distance_meters(lat1, lon1, lat2, lon2):
    from math import radians, sin, cos, sqrt, atan2

    earth_radius = 6371000
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)

    a = (
        sin(dlat / 2) ** 2
        + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    )

    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return earth_radius * c


@app.get("/")
def root():
    return {
        "message": "Digital Twin API is running",
        "stopsFile": str(STOPS_FILE)
    }


def read_route57_scenario_control():
    if not SCENARIO_CONTROL_FILE.exists():
        return {"scenarioId": "R57_ROADWORKS_SARAISHYK"}

    try:
        with SCENARIO_CONTROL_FILE.open("r", encoding="utf-8") as file:
            data = json.load(file)
    except Exception:
        return {"scenarioId": "R57_ROADWORKS_SARAISHYK"}

    scenario_id = str(data.get("scenarioId", "R57_ROADWORKS_SARAISHYK"))
    if scenario_id not in ALLOWED_ROUTE57_SCENARIOS:
        scenario_id = "R57_ROADWORKS_SARAISHYK"

    return {"scenarioId": scenario_id}


@app.get("/api/scenario/route57")
def get_route57_scenario():
    return read_route57_scenario_control()


@app.post("/api/scenario/route57")
def set_route57_scenario(payload: dict = Body(...)):
    scenario_id = str(payload.get("scenarioId", "NONE"))

    if scenario_id not in ALLOWED_ROUTE57_SCENARIOS:
        raise HTTPException(status_code=400, detail="Unsupported Route_57 scenario")

    SCENARIO_CONTROL_FILE.parent.mkdir(parents=True, exist_ok=True)
    with SCENARIO_CONTROL_FILE.open("w", encoding="utf-8") as file:
        json.dump({"scenarioId": scenario_id}, file, ensure_ascii=False, indent=2)

    return {"scenarioId": scenario_id}


@app.get("/api/buses/latest")
def get_latest_buses():
    query = """
    SELECT DISTINCT ON (bus_id)
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
        current_stop_name,
        boarding_count,
        alighting_count,
        waiting_passengers,
        dwell_remaining
    FROM bus_telemetry
    ORDER BY bus_id, id DESC;
    """

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(query)
        rows = cur.fetchall()

        result = []

        for row in rows:
            result.append({
                "busId": row[0],
                "routeId": row[1],
                "timestamp": str(row[2]),
                "latitude": row[3],
                "longitude": row[4],
                "speed": row[5],
                "passengerCount": row[6],
                "doorStatus": row[7],
                "trafficLevel": row[8],
                "eventType": row[9],
                "currentStopName": row[10],
                "boardingCount": row[11] or 0,
                "alightingCount": row[12] or 0,
                "waitingPassengers": row[13] or 0,
                "dwellRemaining": row[14] or 0,
            })

        return result

    finally:
        cur.close()
        conn.close()


@app.get("/api/buses/history")
def get_bus_history(bus_id: str = "Bus_1", limit: int = 5000):
    query = """
    SELECT
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
        current_stop_name,
        boarding_count,
        alighting_count,
        waiting_passengers,
        dwell_remaining
    FROM bus_telemetry
    WHERE bus_id = %s
    ORDER BY id DESC
    LIMIT %s;
    """

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(query, (bus_id, limit))
        rows = cur.fetchall()

        result = []

        for row in rows:
            result.append({
                "busId": row[0],
                "routeId": row[1],
                "timestamp": str(row[2]),
                "latitude": row[3],
                "longitude": row[4],
                "speed": row[5],
                "passengerCount": row[6],
                "doorStatus": row[7],
                "trafficLevel": row[8],
                "eventType": row[9],
                "currentStopName": row[10],
                "boardingCount": row[11] or 0,
                "alightingCount": row[12] or 0,
                "waitingPassengers": row[13] or 0,
                "dwellRemaining": row[14] or 0,
            })

        result.reverse()
        return result

    finally:
        cur.close()
        conn.close()


@app.get("/api/buses/stops")
def get_bus_stops(bus_id: str = "Bus_1"):
    try:
        stops = load_route_stops()
    except Exception as e:
        return {
            "error": str(e),
            "busId": bus_id,
            "currentStopIndex": None,
            "currentStopName": None,
            "nextStopName": None,
            "previousStopName": None,
            "stops": []
        }

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        latest_query = """
        SELECT latitude, longitude, timestamp
        FROM bus_telemetry
        WHERE bus_id = %s
        ORDER BY id DESC
        LIMIT 1;
        """
        cur.execute(latest_query, (bus_id,))
        latest_row = cur.fetchone()

        if not latest_row:
            return {
                "busId": bus_id,
                "currentStopIndex": None,
                "currentStopName": None,
                "nextStopName": None,
                "previousStopName": None,
                "stops": stops
            }

        latest_lat = float(latest_row[0])
        latest_lon = float(latest_row[1])

        nearest_index = None
        nearest_distance = None

        for i, stop in enumerate(stops):
            dist = distance_meters(
                latest_lat,
                latest_lon,
                stop["latitude"],
                stop["longitude"]
            )

            if nearest_distance is None or dist < nearest_distance:
                nearest_distance = dist
                nearest_index = i

        current_stop_name = stops[nearest_index]["name"] if nearest_index is not None else None
        previous_stop_name = (
            stops[nearest_index - 1]["name"]
            if nearest_index is not None and nearest_index > 0
            else None
        )
        next_stop_name = (
            stops[nearest_index + 1]["name"]
            if nearest_index is not None and nearest_index < len(stops) - 1
            else None
        )

        return {
            "busId": bus_id,
            "currentStopIndex": nearest_index,
            "currentStopName": current_stop_name,
            "nextStopName": next_stop_name,
            "previousStopName": previous_stop_name,
            "nearestStopDistanceMeters": round(nearest_distance, 1) if nearest_distance is not None else None,
            "stops": stops
        }

    finally:
        cur.close()
        conn.close()


@app.get("/api/system/overview")
def get_system_overview():
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        latest_query = """
        SELECT DISTINCT ON (bus_id)
            bus_id,
            route_id,
            speed
        FROM bus_telemetry
        ORDER BY bus_id, id DESC;
        """

        cur.execute(latest_query)
        latest_rows = cur.fetchall()

        active_buses = len(latest_rows)
        unique_routes = len(set(row[1] for row in latest_rows))
        avg_speed = 0.0

        if active_buses > 0:
            avg_speed = round(sum(float(row[2]) for row in latest_rows) / active_buses, 2)

        history_query = """
        WITH ranked AS (
            SELECT
                bus_id,
                traffic_level,
                passenger_count,
                ROW_NUMBER() OVER (PARTITION BY bus_id ORDER BY id DESC) AS rn
            FROM bus_telemetry
        )
        SELECT
            bus_id,
            traffic_level,
            passenger_count
        FROM ranked
        WHERE rn <= 20;
        """

        cur.execute(history_query)
        rows = cur.fetchall()

        bus_stats = {}

        for bus_id, traffic_level, passenger_count in rows:
            if bus_id not in bus_stats:
                bus_stats[bus_id] = {
                    "high_traffic_count": 0,
                    "overcrowded_count": 0
                }

            if traffic_level == "HIGH":
                bus_stats[bus_id]["high_traffic_count"] += 1

            if passenger_count is not None and int(passenger_count) >= 50:
                bus_stats[bus_id]["overcrowded_count"] += 1

        high_traffic_bus_ids = [
            bus_id for bus_id, stats in bus_stats.items()
            if stats["high_traffic_count"] >= 3
        ]

        overcrowded_bus_ids = [
            bus_id for bus_id, stats in bus_stats.items()
            if stats["overcrowded_count"] >= 3
        ]

        return {
            "activeBuses": active_buses,
            "routes": unique_routes,
            "avgNetworkSpeed": avg_speed,
            "highTrafficBusesCount": len(high_traffic_bus_ids),
            "highTrafficBusIds": high_traffic_bus_ids,
            "overcrowdedBusesCount": len(overcrowded_bus_ids),
            "overcrowdedBusIds": overcrowded_bus_ids,
            "mode": "Simulation"
        }

    finally:
        cur.close()
        conn.close()


@app.get("/api/weather/astana")
def get_astana_weather():
    try:
        city_name = "Astana"

        geocoding_url = (
            "https://geocoding-api.open-meteo.com/v1/search"
            f"?name={quote(city_name)}&count=1&language=en&format=json"
        )

        with urlopen(geocoding_url) as response:
            geo_data = json.loads(response.read().decode("utf-8"))

        results = geo_data.get("results", [])

        if not results:
            return {"error": "City not found"}

        latitude = results[0]["latitude"]
        longitude = results[0]["longitude"]

        weather_url = (
            "https://api.open-meteo.com/v1/forecast"
            f"?latitude={latitude}&longitude={longitude}"
            "&current=temperature_2m,weather_code,wind_speed_10m"
            "&daily=temperature_2m_max,temperature_2m_min,weather_code"
            "&timezone=auto"
        )

        with urlopen(weather_url) as response:
            weather_data = json.loads(response.read().decode("utf-8"))

        current = weather_data.get("current", {})
        daily = weather_data.get("daily", {})

        return {
            "city": "Astana",
            "currentTemperature": current.get("temperature_2m"),
            "currentWindSpeed": current.get("wind_speed_10m"),
            "currentWeatherCode": current.get("weather_code"),
            "todayMin": daily.get("temperature_2m_min", [None])[0],
            "todayMax": daily.get("temperature_2m_max", [None])[0]
        }

    except Exception as e:
        return {"error": str(e)}


@app.get("/api/buses/trends")
def get_bus_trends(bus_id: str = "Bus_1", limit: int = 20):
    query = """
    SELECT
        timestamp,
        speed,
        passenger_count
    FROM bus_telemetry
    WHERE bus_id = %s
    ORDER BY id DESC
    LIMIT %s;
    """

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(query, (bus_id, limit))
        rows = cur.fetchall()

        if not rows:
            return {"error": "No data"}

        rows.reverse()

        result = {
            "busId": bus_id,
            "timestamps": [],
            "speeds": [],
            "passengers": []
        }

        for row in rows:
            try:
                result["timestamps"].append(str(row[0]))
                result["speeds"].append(round(float(row[1]) * 3.6, 1))
                result["passengers"].append(int(row[2]))
            except Exception:
                continue

        if not result["speeds"] and not result["passengers"]:
            return {"error": "No data"}

        return result

    finally:
        cur.close()
        conn.close()


@app.get("/api/buses/analytics")
def get_bus_analytics(bus_id: str = "Bus_1"):
    query = """
    SELECT
        speed,
        passenger_count,
        event_type,
        traffic_level,
        timestamp
    FROM bus_telemetry
    WHERE bus_id = %s
    ORDER BY id ASC;
    """

    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(query, (bus_id,))
        rows = cur.fetchall()

        if not rows:
            return {"error": "No data"}

        speeds = []
        passengers = []

        event_counts = {
            "MOVING": 0,
            "STOP": 0,
            "TRAFFIC": 0,
            "IDLE": 0
        }

        traffic_counts = {
            "LOW": 0,
            "MEDIUM": 0,
            "HIGH": 0
        }

        valid_rows = []

        for row in rows:
            try:
                speed = float(row[0])
                passenger = int(row[1])
                event = row[2] if row[2] else "UNKNOWN"
                traffic = row[3] if row[3] else "UNKNOWN"
            except Exception:
                continue

            speeds.append(speed)
            passengers.append(passenger)
            valid_rows.append({
                "speed": speed,
                "passenger": passenger,
                "event": event,
                "traffic": traffic
            })

            if event in event_counts:
                event_counts[event] += 1

            if traffic in traffic_counts:
                traffic_counts[traffic] += 1

        if not valid_rows:
            return {"error": "No valid data"}

        avg_speed = round(sum(speeds) / len(speeds), 2)
        avg_passengers = int(sum(passengers) / len(passengers))
        total = len(valid_rows)

        event_percentages = {
            key: round((value / total) * 100, 1)
            for key, value in event_counts.items()
        }

        traffic_percentages = {
            key: round((value / total) * 100, 1)
            for key, value in traffic_counts.items()
        }

        latest_row = valid_rows[-1]
        current_traffic = latest_row["traffic"]
        current_event = latest_row["event"]

        stop_traffic_ratio = event_percentages["STOP"] + event_percentages["TRAFFIC"]

        if current_traffic == "HIGH" or avg_speed < 3 or stop_traffic_ratio >= 40:
            delay_risk = "HIGH"
        elif current_traffic == "MEDIUM" or avg_speed < 6 or stop_traffic_ratio >= 20:
            delay_risk = "MEDIUM"
        else:
            delay_risk = "LOW"

        if current_traffic == "HIGH":
            operational_status = "CONGESTED"
        elif avg_passengers >= 50:
            operational_status = "CROWDED"
        elif delay_risk == "HIGH":
            operational_status = "DELAY_RISK"
        else:
            operational_status = "NORMAL"

        efficiency_score = 100
        efficiency_score -= event_percentages["TRAFFIC"] * 0.8
        efficiency_score -= event_percentages["STOP"] * 0.5

        if avg_speed < 3:
            efficiency_score -= 25
        elif avg_speed < 6:
            efficiency_score -= 12

        if delay_risk == "HIGH":
            efficiency_score -= 20
        elif delay_risk == "MEDIUM":
            efficiency_score -= 10

        if avg_passengers >= 50:
            efficiency_score -= 12

        efficiency_score = max(0, min(100, round(efficiency_score, 1)))

        if efficiency_score >= 80:
            efficiency_label = "GOOD"
        elif efficiency_score >= 55:
            efficiency_label = "MODERATE"
        else:
            efficiency_label = "POOR"

        return {
            "busId": bus_id,
            "avgSpeed": avg_speed,
            "avgPassengers": avg_passengers,
            "currentTrafficLevel": current_traffic,
            "currentEventType": current_event,
            "delayRisk": delay_risk,
            "operationalStatus": operational_status,
            "eventDistribution": event_percentages,
            "trafficDistribution": traffic_percentages,
            "routeEfficiencyScore": efficiency_score,
            "routeEfficiencyLabel": efficiency_label
        }

    finally:
        cur.close()
        conn.close()
