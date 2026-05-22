import time
import json
import random
from pathlib import Path
from datetime import datetime

import pandas as pd
import paho.mqtt.client as mqtt


# ==================================================
# MQTT SETTINGS
# ==================================================
BROKER = "127.0.0.1"
PORT = 1883
TOPIC = "bus/telemetry"
CITY_TOPIC = "city/bus/telemetry"
MQTT_TOPICS = [TOPIC, CITY_TOPIC]


# ==================================================
# FILE PATHS
# ==================================================
BASE_DIR = Path(__file__).resolve().parent

ROUTE_DIR = BASE_DIR / "data" / "routes" / "route_57"
SHAPE_FILE = ROUTE_DIR / "route_57_shape.csv"
STOPS_FILE = ROUTE_DIR / "route_57_stops.csv"

# Old GPS file is used ONLY as a real speed profile.
# It is NOT used as the route geometry anymore.
REAL_SPEED_FILE = BASE_DIR / "gps_clean_full.csv"

# Old observed event/load file is used ONLY as a real passenger load profile.
# It is NOT used as route geometry anymore.
REAL_EVENTS_FILE = BASE_DIR / "events_with_real_gps.csv"


# ==================================================
# SIMULATION SETTINGS
# ==================================================
PUBLISH_INTERVAL_SECONDS = 1
BUS_CAPACITY = 60
MIN_DWELL_TICKS = 4
MAX_DWELL_TICKS = 9
FALLBACK_SPEED_MPS = 8.0
FALLBACK_LOAD_LEVEL = "MID"

# Passenger realism.
# The previous model could create unrealistic changes like -50 alighted at one stop.
# These caps keep stop-level passenger exchange believable.
MAX_ALIGHTING_PER_STOP = 14
MAX_ALIGHTING_FOCUS_STOP = 22
MAX_ALIGHTING_FINAL_STOP = BUS_CAPACITY

MAX_BOARDING_PER_STOP = 20
MAX_BOARDING_FOCUS_STOP = 28
MAX_BOARDING_FIRST_STOP = 36

# Dwell time realism.
# events_with_real_gps.csv duration column is used when available for STOP events.
MIN_DWELL_SECONDS = 8
MAX_DWELL_SECONDS = 90
FALLBACK_DWELL_SECONDS = 18

# Support bus realism.
SUPPORT_BUS_ENABLED = True
SUPPORT_BUS_CAPACITY = 60
SUPPORT_BUS_TRIGGER_QUEUE = 5
SUPPORT_BUS_ROUTE_ID = "Route_57_SUPPORT"

# Prevent infinite / chaotic support bus spawning.
# For a clean diploma demo, keep one active support bus at a time.
MAX_ACTIVE_SUPPORT_BUSES = 2

# Support is allowed only at important/high-demand locations.
# This prevents support buses from appearing at every minor stop.
SUPPORT_CRITICAL_STOP_NAMES = [
    "Улица Сарайшык",
    "Министерство иностранных дел",
    "Бизнес-центр Downtown",
    "Казахско-турецкий лицей",
    "ЖК Астана Сани",
    "Улица Мухамеджана Тынышбаева",
    "Вокзал Нурлы жол"
]

# Named standby points on the route.
# They are mapped to real stops from route_57_stops.csv and then to route_57_shape.csv.
SUPPORT_RESERVE_ANCHOR_STOPS = [
    {
        "reserveName": "Reserve A — Route Start / ЖК Ак Дидар",
        "anchorStopName": "ЖК Ак Дидар"
    },
    {
        "reserveName": "Reserve B — Green Quarter / Зеленый квартал",
        "anchorStopName": "Резидентский комплекс Зеленый квартал"
    },
    {
        "reserveName": "Reserve C — Downtown / Business Center",
        "anchorStopName": "Бизнес-центр Downtown"
    },
    {
        "reserveName": "Reserve D — Astana Sani",
        "anchorStopName": "ЖК Астана Сани"
    }
]

# A reserve point must be at least this many route-shape points before the target stop.
SUPPORT_MIN_UPSTREAM_GAP_POINTS = 35

# Fallback only if named reserve mapping fails.
SUPPORT_RESERVE_FRACTIONS = [0.0, 0.25, 0.50, 0.75]
SUPPORT_FALLBACK_LOOKBACK_POINTS = 120

# If the nearest named reserve is too far from the overloaded stop,
# use a short upstream standby segment instead.
# This represents a reserve bus already staged near the route section,
# not a bus teleporting from the same far depot every time.
SUPPORT_MAX_NAMED_RESERVE_DISTANCE_POINTS = 95
SUPPORT_DYNAMIC_LOOKBACK_POINTS = 55

# After the first rescue pickup, support bus serves residual passenger demand.
# It must NOT behave like another fully demanded main bus.
SUPPORT_RESIDUAL_BOARDING_MULTIPLIER = 0.45
SUPPORT_NORMAL_MAX_BOARDING = 6
SUPPORT_FOCUS_MAX_BOARDING = 12
SUPPORT_NORMAL_MAX_ALIGHTING = 5
SUPPORT_FOCUS_MAX_ALIGHTING = 8
SUPPORT_SOFT_COMFORT_LOAD = 45
SUPPORT_HARD_DISPATCH_LOAD = 58
SUPPORT_REUSE_MAX_GAP_POINTS = None

# Passenger target ranges are based on your observed events_with_real_gps.csv.
# They represent desired onboard load for Route_57, not all people at a stop.
LOAD_TARGET_RANGES = {
    "ZERO": (0, 3),
    "LOW": (6, 18),
    "MID": (24, 40),
    "HIGH": (45, 66)
}


# ==================================================
# BUS LIFECYCLE / DISPATCH STATES
# ==================================================
STATE_WAITING_TO_START = "WAITING_TO_START"
STATE_IN_SERVICE = "IN_SERVICE"
STATE_STOPPING_AT_STOP = "STOPPING_AT_STOP"
STATE_EN_ROUTE_TO_UNSERVED_QUEUE = "EN_ROUTE_TO_UNSERVED_QUEUE"
STATE_BOARDING_UNSERVED_QUEUE = "BOARDING_UNSERVED_QUEUE"
STATE_CONTINUING_ROUTE_AFTER_PICKUP = "CONTINUING_ROUTE_AFTER_PICKUP"
STATE_COMPLETED_ROUTE = "COMPLETED_ROUTE"


# ==================================================
# ROUTE CONFIGURATION
# ==================================================
# Route_57 remains the stable reference route. Future routes should be added
# here with their own files, headway, critical stops, reserve points, and rules
# instead of copying the simulator logic.
ROUTE_CONFIGS = {
    "Route_57": {
        "route_id": "Route_57",
        "route_shape_file": SHAPE_FILE,
        "route_stops_file": STOPS_FILE,
        "passenger_profile_file": REAL_EVENTS_FILE,
        "speed_profile_file": REAL_SPEED_FILE,
        "capacity": BUS_CAPACITY,
        "critical_stops": SUPPORT_CRITICAL_STOP_NAMES,
        "reserve_points": SUPPORT_RESERVE_ANCHOR_STOPS,
        "terminal_taper_rules": {
            "final_stop_allows_full_alighting": True,
            "zero_load_max_alighting": 24
        },
        "support_bus_rules": {
            "enabled": SUPPORT_BUS_ENABLED,
            "support_route_id": SUPPORT_BUS_ROUTE_ID,
            "capacity": SUPPORT_BUS_CAPACITY,
            "trigger_queue": SUPPORT_BUS_TRIGGER_QUEUE,
            "max_active_support_buses": MAX_ACTIVE_SUPPORT_BUSES,
            "reuse_max_gap_points": SUPPORT_REUSE_MAX_GAP_POINTS,
            "residual_boarding_multiplier": SUPPORT_RESIDUAL_BOARDING_MULTIPLIER
        },
        "dispatch_schedule": [
            {
                "bus_id": "Bus_1",
                "start_delay_seconds": 0,
                "start_jitter_seconds": 0,
                "passenger_scale": 1.0
            }
        ],
        "headway_seconds": 420,
        "start_jitter_seconds": 45,
        "scenario_mode": "PEAK_HOUR"
    }
}

ACTIVE_ROUTE_ID = "Route_57"
ACTIVE_ROUTE_CONFIG = ROUTE_CONFIGS[ACTIVE_ROUTE_ID]


# ==================================================
# REROUTING DECISION PREPARATION
# ==================================================
REROUTE_RULES = {
    "upcoming_window_points": 18,
    "high_traffic_segments_threshold": 5,
    "average_speed_threshold_mps": 3.2,
    "alternative_route_exists": False
}


# ==================================================
# SCENARIO ENGINE
# ==================================================
# NORMAL        = realistic baseline based on your observed data.
# PEAK_HOUR     = realistic stress scenario for demonstrating efficiency decisions.
# STATION_EVENT = strong demand near terminal / station area.
#
# For diploma demo, use PEAK_HOUR.
# For baseline comparison, switch to NORMAL.
SCENARIO_MODE = "PEAK_HOUR"

SCENARIO_FOCUS_STOPS = [
    "Улица Сарайшык",
    "Министерство иностранных дел",
    "Бизнес-центр Downtown",
    "Казахско-турецкий лицей",
    "ЖК Астана Сани",
    "Улица Мухамеджана Тынышбаева",
    "Вокзал Нурлы жол"
]

SCENARIO_CONFIG = {
    "NORMAL": {
        "enabled": False,
        "description": "Baseline real passenger profile",
        "high_min_target": 0,
        "focus_min_queue": 0,
        "focus_extra_min": 0,
        "focus_extra_max": 0
    },
    "PEAK_HOUR": {
        "enabled": True,
        "description": "Peak-hour stress scenario with overloaded stops",
        "high_min_target": 58,
        "focus_min_queue": 8,
        "focus_extra_min": 8,
        "focus_extra_max": 16
    },
    "STATION_EVENT": {
        "enabled": True,
        "description": "Event demand near station / terminal",
        "high_min_target": 60,
        "focus_min_queue": 14,
        "focus_extra_min": 14,
        "focus_extra_max": 24
    }
}

SCENARIO_MODE = str(ACTIVE_ROUTE_CONFIG.get("scenario_mode", SCENARIO_MODE))


# ==================================================
# ONLY ONE MAIN BUS FOR NOW
# Support buses will be added later by decision logic.
# ==================================================
BUSES = [
    {
        "busId": "Bus_1",
        "routeId": "Route_57",
        "current_index": 0,
        "passengerScale": 1.0,
        "passengerCount": 0,
        "capacity": BUS_CAPACITY,
        "busRole": "MAIN",
        "active": True,
        "dwellRemaining": 0,
        "nextStopIndex": 0,
        "currentStopName": None,
        "lastBoarding": 0,
        "lastAlighting": 0,
        "lastWaiting": 0,
        "lastLoadLevel": None,
        "targetPassengers": None,
        "state": STATE_WAITING_TO_START,
        "plannedStartDelaySeconds": 0,
        "startJitterSeconds": 0,
        "effectiveStartDelaySeconds": 0,
        "startedAtSimulationSecond": None,
    }
]


# ==================================================
# SUPPORT BUS / UNSERVED QUEUE STATE
# ==================================================
# Key: stopSequence, value: passengers left behind for Route_57.
UNSERVED_QUEUES_BY_STOP = {}

# Stops for which a support bus has already been dispatched.
DISPATCHED_SUPPORT_STOPS = set()

SUPPORT_BUS_COUNTER = 0

# Filled in main() after route_57_shape.csv is loaded.
ROUTE_POINT_COUNT = 0

# Filled in main() after route stops are mapped.
SUPPORT_RESERVE_POINTS = []


# ==================================================
# CSV HELPERS
# ==================================================
def get_active_route_file(key: str, fallback: Path) -> Path:
    return Path(ACTIVE_ROUTE_CONFIG.get(key) or fallback)


def get_support_rule(key: str, fallback):
    return ACTIVE_ROUTE_CONFIG.get("support_bus_rules", {}).get(key, fallback)


def read_csv_auto(path: Path) -> pd.DataFrame:
    """Read CSV files exported from Excel/browser with ;, comma, or auto delimiter."""
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    for separator in [";", ","]:
        try:
            df = pd.read_csv(path, sep=separator, encoding="utf-8-sig")
            if len(df.columns) > 1:
                return df
        except Exception:
            pass

    return pd.read_csv(path, sep=None, engine="python", encoding="utf-8-sig")


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip().str.lower()
    return df


def to_number(series: pd.Series) -> pd.Series:
    """Convert numeric columns and support comma decimals if Excel changed them."""
    return pd.to_numeric(
        series.astype(str).str.replace(",", ".", regex=False).str.strip(),
        errors="coerce"
    )


# ==================================================
# ROUTE DATA LOADING
# ==================================================
def load_route_shape() -> pd.DataFrame:
    shape_file = get_active_route_file("route_shape_file", SHAPE_FILE)
    df = normalize_columns(read_csv_auto(shape_file))

    required_columns = {"point_sequence", "latitude", "longitude"}
    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"{shape_file.name} is missing columns: {missing_columns}. "
            f"Current columns: {df.columns.tolist()}"
        )

    df["point_sequence"] = to_number(df["point_sequence"])
    df["latitude"] = to_number(df["latitude"])
    df["longitude"] = to_number(df["longitude"])

    df = df.dropna(subset=["point_sequence", "latitude", "longitude"])
    df = df.sort_values("point_sequence").reset_index(drop=True)

    if len(df) < 2:
        raise ValueError(f"{shape_file.name} must contain at least 2 route points.")

    return df


def load_route_stops() -> pd.DataFrame:
    stops_file = get_active_route_file("route_stops_file", STOPS_FILE)
    df = normalize_columns(read_csv_auto(stops_file))

    required_columns = {"stop_sequence", "stop_name", "latitude", "longitude"}
    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"{stops_file.name} is missing columns: {missing_columns}. "
            f"Current columns: {df.columns.tolist()}"
        )

    if "demand_level" not in df.columns:
        df["demand_level"] = "MEDIUM"

    df["stop_sequence"] = to_number(df["stop_sequence"])
    df["latitude"] = to_number(df["latitude"])
    df["longitude"] = to_number(df["longitude"])
    df["stop_name"] = df["stop_name"].astype(str).str.strip()
    df["demand_level"] = df["demand_level"].fillna("MEDIUM").astype(str).str.strip().str.upper()

    df = df.dropna(subset=["stop_sequence", "stop_name", "latitude", "longitude"])
    df = df.sort_values("stop_sequence").reset_index(drop=True)

    if len(df) < 1:
        raise ValueError(f"{stops_file.name} must contain at least 1 stop.")

    return df


# ==================================================
# REAL SPEED PROFILE FROM OLD GPS DATA
# ==================================================
def resample_values(values, target_length: int):
    """Resample a list to target_length without requiring numpy."""
    if target_length <= 0:
        return []

    if not values:
        return [FALLBACK_SPEED_MPS] * target_length

    if len(values) == 1:
        return [values[0]] * target_length

    result = []
    old_last = len(values) - 1
    new_last = max(target_length - 1, 1)

    for new_index in range(target_length):
        position = (new_index / new_last) * old_last
        left_index = int(position)
        right_index = min(left_index + 1, old_last)
        fraction = position - left_index

        interpolated = values[left_index] * (1 - fraction) + values[right_index] * fraction
        result.append(round(float(interpolated), 2))

    return result


def load_real_speed_profile(target_length: int):
    """
    Use old gps_clean_full.csv only as a real speed profile.
    The new route shape still controls bus coordinates.
    Speed is stored in m/s. Frontend converts speed to km/h using * 3.6.
    """
    if target_length <= 0:
        return []

    speed_file = get_active_route_file("speed_profile_file", REAL_SPEED_FILE)

    if not speed_file.exists():
        print(f"WARNING: {speed_file.name} not found. Using fallback speed profile.")
        return [FALLBACK_SPEED_MPS] * target_length

    try:
        df = normalize_columns(read_csv_auto(speed_file))
    except Exception as error:
        print(f"WARNING: failed to read {speed_file.name}: {error}. Using fallback speed profile.")
        return [FALLBACK_SPEED_MPS] * target_length

    if "speed" not in df.columns:
        print(f"WARNING: speed column not found in {speed_file.name}. Using fallback speed profile.")
        return [FALLBACK_SPEED_MPS] * target_length

    speeds = to_number(df["speed"]).dropna()
    speeds = speeds[(speeds >= 0) & (speeds <= 25)]

    if len(speeds) < 2:
        print("WARNING: not enough valid speed data. Using fallback speed profile.")
        return [FALLBACK_SPEED_MPS] * target_length

    # Smooth real speed to avoid unrealistic sharp jumps.
    speeds = speeds.rolling(window=5, min_periods=1, center=True).median()
    speed_profile = resample_values(speeds.tolist(), target_length)

    return speed_profile


def adapt_speed_for_bus(base_speed: float) -> float:
    """Keep real speed profile, with tiny natural variation."""
    jitter = random.uniform(-0.25, 0.25)
    speed = base_speed + jitter

    # City bus safety limit: max 18 m/s = 64.8 km/h.
    speed = max(0.0, min(speed, 18.0))

    return round(speed, 2)


def speed_to_traffic_level(speed: float) -> str:
    """Convert bus speed in m/s into dashboard traffic level."""
    speed = float(speed)

    if speed <= 3.5:
        return "HIGH"
    if speed <= 8.0:
        return "MEDIUM"
    return "LOW"


def normalize_load_level(value) -> str:
    load_level = str(value).strip().upper()

    if load_level in {"ZERO", "EMPTY", "0"}:
        return "ZERO"
    if load_level in {"LOW", "L"}:
        return "LOW"
    if load_level in {"MID", "MEDIUM", "M"}:
        return "MID"
    if load_level in {"HIGH", "H"}:
        return "HIGH"

    return FALLBACK_LOAD_LEVEL


def deterministic_target_for_load(load_level: str, route_index: int) -> int:
    """
    Converts observed load level into a stable passenger target.
    Stable means the same route point gets the same target on every run.
    """
    level = normalize_load_level(load_level)
    low, high = LOAD_TARGET_RANGES.get(level, LOAD_TARGET_RANGES[FALLBACK_LOAD_LEVEL])

    seed = 57000 + int(route_index) * 17 + sum(ord(ch) for ch in level) * 31
    rng = random.Random(seed)

    return int(rng.randint(low, high))


def load_real_passenger_events():
    """
    Reads events_with_real_gps.csv as a real passenger load profile.
    Expected columns:
    timestamp;event;load;duration;latitude;longitude;speed
    """
    passenger_file = get_active_route_file("passenger_profile_file", REAL_EVENTS_FILE)

    if not passenger_file.exists():
        print(f"WARNING: {passenger_file.name} not found. Using fallback MID passenger profile.")
        return None

    try:
        df = normalize_columns(read_csv_auto(passenger_file))
    except Exception as error:
        print(f"WARNING: failed to read {passenger_file.name}: {error}. Using fallback MID passenger profile.")
        return None

    required_columns = {"load", "latitude", "longitude"}
    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        print(
            f"WARNING: {passenger_file.name} missing columns:",
            missing_columns,
            "Using fallback MID passenger profile."
        )
        return None

    df["latitude"] = to_number(df["latitude"])
    df["longitude"] = to_number(df["longitude"])
    df["load"] = df["load"].apply(normalize_load_level)

    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(
            df["timestamp"],
            format="%d.%m.%Y %H:%M",
            errors="coerce"
        )
        df = df.sort_values("timestamp").reset_index(drop=True)

    df = df.dropna(subset=["latitude", "longitude", "load"]).reset_index(drop=True)

    if len(df) == 0:
        print("WARNING: no valid passenger events found. Using fallback MID passenger profile.")
        return None

    return df


def build_passenger_load_profile(route_df: pd.DataFrame):
    """
    Maps observed load events to the new cleaned route shape.
    The route shape controls coordinates; events file controls passenger load behavior.
    """
    events_df = load_real_passenger_events()
    route_length = len(route_df)

    if route_length <= 0:
        return [], []

    if events_df is None:
        load_levels = [FALLBACK_LOAD_LEVEL] * route_length
        targets = [
            deterministic_target_for_load(FALLBACK_LOAD_LEVEL, index)
            for index in range(route_length)
        ]
        return load_levels, targets

    mapped_events = []

    for _, event in events_df.iterrows():
        best_index = None
        best_distance = None

        for route_index, point in route_df.iterrows():
            dist = distance_meters(
                float(point["latitude"]),
                float(point["longitude"]),
                float(event["latitude"]),
                float(event["longitude"])
            )

            if best_distance is None or dist < best_distance:
                best_distance = dist
                best_index = route_index

        mapped_events.append({
            "routeIndex": int(best_index),
            "loadLevel": normalize_load_level(event["load"]),
            "distanceToRoute": float(best_distance)
        })

    mapped_events = sorted(mapped_events, key=lambda item: item["routeIndex"])

    print("\nPassenger load events mapped to route:")
    for event in mapped_events:
        print(
            f"routeIndex={event['routeIndex']} | "
            f"load={event['loadLevel']} | "
            f"distance={event['distanceToRoute']:.1f}m"
        )

    # Fill every route point with the latest observed load level.
    load_levels = []
    current_load = mapped_events[0]["loadLevel"]
    event_pointer = 0

    for route_index in range(route_length):
        while (
            event_pointer < len(mapped_events)
            and route_index >= mapped_events[event_pointer]["routeIndex"]
        ):
            current_load = mapped_events[event_pointer]["loadLevel"]
            event_pointer += 1

        load_levels.append(current_load)

    targets = [
        deterministic_target_for_load(load_level, route_index)
        for route_index, load_level in enumerate(load_levels)
    ]

    return load_levels, targets


def get_route_target_passengers(route_row, route_index: int) -> int:
    value = route_row.get("target_passengers", None)

    if value is None or pd.isna(value):
        return deterministic_target_for_load(FALLBACK_LOAD_LEVEL, route_index)

    return int(value)


def get_route_load_level(route_row) -> str:
    value = route_row.get("load_level", FALLBACK_LOAD_LEVEL)
    return normalize_load_level(value)


def normalize_text_for_matching(value) -> str:
    return str(value or "").strip().lower().replace("ё", "е")


def is_scenario_focus_stop(stop_name: str) -> bool:
    normalized_stop = normalize_text_for_matching(stop_name)

    return any(
        normalize_text_for_matching(keyword) in normalized_stop
        for keyword in SCENARIO_FOCUS_STOPS
    )


def get_scenario_config() -> dict:
    return SCENARIO_CONFIG.get(SCENARIO_MODE, SCENARIO_CONFIG["NORMAL"])


def apply_passenger_scenario(
    base_target: int,
    stop: dict,
    load_level: str,
    route_index: int
) -> int:
    """
    Applies a what-if scenario over the real observed passenger profile.

    Important:
    - NORMAL keeps your real observed load behavior.
    - PEAK_HOUR / STATION_EVENT increase demand only at logical focus stops.
    - target > BUS_CAPACITY creates route queue, which is the basis for support bus decision.
    """
    config = get_scenario_config()

    if not config["enabled"]:
        return int(base_target)

    stop_name = stop.get("name", "")
    stop_sequence = int(stop.get("stopSequence", 0))
    load_level = normalize_load_level(load_level)

    target = int(base_target)

    # Keep high-load areas close to full even if the real sampled value was lower.
    if load_level == "HIGH":
        target = max(target, int(config["high_min_target"]))

    # Focus stops represent places where overload is likely during peak hour.
    if is_scenario_focus_stop(stop_name) and load_level in {"MID", "HIGH"}:
        seed = 88000 + route_index * 13 + stop_sequence * 101 + len(SCENARIO_MODE) * 19
        rng = random.Random(seed)
        extra_demand = rng.randint(
            int(config["focus_extra_min"]),
            int(config["focus_extra_max"])
        )

        target = max(
            target + extra_demand,
            BUS_CAPACITY + int(config["focus_min_queue"])
        )

    return int(target)


# ==================================================
# ROUTE / STOP MAPPING
# ==================================================
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


def map_stops_to_route(route_df: pd.DataFrame, stops_df: pd.DataFrame):
    mapped_stops = []

    for _, stop in stops_df.iterrows():
        best_index = None
        best_distance = None

        for route_index, point in route_df.iterrows():
            distance = distance_meters(
                float(point["latitude"]),
                float(point["longitude"]),
                float(stop["latitude"]),
                float(stop["longitude"])
            )

            if best_distance is None or distance < best_distance:
                best_distance = distance
                best_index = route_index

        mapped_stops.append({
            "stopSequence": int(stop["stop_sequence"]),
            "name": str(stop["stop_name"]),
            "latitude": float(stop["latitude"]),
            "longitude": float(stop["longitude"]),
            "demandLevel": str(stop["demand_level"]).upper(),
            "routeIndex": int(best_index),
            "distanceToRoute": float(best_distance)
        })

    mapped_stops = sorted(mapped_stops, key=lambda item: item["routeIndex"])

    for stop_index, mapped_stop in enumerate(mapped_stops):
        mapped_stop["stopIndex"] = stop_index
        mapped_stop["isFirstStop"] = stop_index == 0
        mapped_stop["isFinalStop"] = stop_index == len(mapped_stops) - 1

    print("\nStops mapped to route:")
    for stop in mapped_stops:
        print(
            f"{stop['stopSequence']}. {stop['name']} | "
            f"routeIndex={stop['routeIndex']} | "
            f"distance={stop['distanceToRoute']:.1f}m | "
            f"demand={stop['demandLevel']}"
        )

    return mapped_stops


def initialize_bus_positions(stops):
    for bus in BUSES:
        bus["current_index"] = 0
        bus["nextStopIndex"] = 0

        for index, stop in enumerate(stops):
            if stop["routeIndex"] >= bus["current_index"]:
                bus["nextStopIndex"] = index
                break
        else:
            bus["nextStopIndex"] = len(stops)

        print(
            f"{bus['busId']} starts at route index {bus['current_index']}, "
            f"nextStopIndex={bus['nextStopIndex']}"
        )


def create_scheduled_main_bus(schedule_item: dict, route_config: dict) -> dict:
    return {
        "busId": schedule_item["bus_id"],
        "routeId": route_config["route_id"],
        "current_index": 0,
        "passengerScale": float(schedule_item.get("passenger_scale", 1.0)),
        "passengerCount": 0,
        "capacity": int(route_config.get("capacity", BUS_CAPACITY)),
        "busRole": "MAIN",
        "active": True,
        "dwellRemaining": 0,
        "nextStopIndex": 0,
        "currentStopName": None,
        "lastBoarding": 0,
        "lastAlighting": 0,
        "lastWaiting": 0,
        "lastLoadLevel": None,
        "targetPassengers": None,
        "state": STATE_WAITING_TO_START,
        "plannedStartDelaySeconds": 0,
        "startJitterSeconds": 0,
        "effectiveStartDelaySeconds": 0,
        "startedAtSimulationSecond": None,
    }


def apply_dispatch_schedule(route_config: dict, stops):
    """
    Applies scheduled starts without changing Route_57 baseline behavior.
    Bus_1 has zero delay today; future buses can be added to the route config
    with independent delay/headway/jitter and will not publish until started.
    """
    schedule = route_config.get("dispatch_schedule") or []
    existing_by_id = {bus["busId"]: bus for bus in BUSES}
    rng = random.Random(20260522 + sum(ord(ch) for ch in route_config["route_id"]))

    for item in schedule:
        bus_id = item["bus_id"]
        bus = existing_by_id.get(bus_id)

        if bus is None:
            bus = create_scheduled_main_bus(item, route_config)
            BUSES.append(bus)
            existing_by_id[bus_id] = bus

        planned_delay = int(item.get("start_delay_seconds", 0))
        jitter_seconds = int(item.get("start_jitter_seconds", route_config.get("start_jitter_seconds", 0)))
        effective_delay = planned_delay

        if jitter_seconds > 0:
            effective_delay += rng.randint(0, jitter_seconds)

        bus["routeId"] = route_config["route_id"]
        bus["capacity"] = int(route_config.get("capacity", BUS_CAPACITY))
        bus["passengerScale"] = float(item.get("passenger_scale", bus.get("passengerScale", 1.0)))
        bus["plannedStartDelaySeconds"] = planned_delay
        bus["startJitterSeconds"] = jitter_seconds
        bus["effectiveStartDelaySeconds"] = effective_delay
        bus["startedAtSimulationSecond"] = None
        bus["state"] = STATE_WAITING_TO_START

        for index, stop in enumerate(stops):
            if stop["routeIndex"] >= bus["current_index"]:
                bus["nextStopIndex"] = index
                break
        else:
            bus["nextStopIndex"] = len(stops)

        print(
            f"DISPATCH PLAN {bus_id}: start_delay={effective_delay}s "
            f"(base={planned_delay}s, jitter={jitter_seconds}s)"
        )


def activate_bus_if_ready(bus, simulation_second: int) -> bool:
    if bus.get("state") != STATE_WAITING_TO_START:
        return True

    start_delay = int(bus.get("effectiveStartDelaySeconds", 0))

    if int(simulation_second) < start_delay:
        return False

    bus["state"] = STATE_IN_SERVICE
    bus["startedAtSimulationSecond"] = int(simulation_second)
    print(f"START {bus['busId']} at simulation_second={simulation_second}")
    return True


# ==================================================
# PASSENGER / DWELL / SUPPORT BUS LOGIC
# ==================================================
def get_stop_key(stop) -> int:
    return int(stop["stopSequence"])


def get_next_stop(bus, stops):
    next_index = bus.get("nextStopIndex", 0)

    if next_index >= len(stops):
        return None

    return stops[next_index]


def get_stop_by_sequence(stops, stop_sequence: int):
    for stop in stops:
        if int(stop["stopSequence"]) == int(stop_sequence):
            return stop

    return None


def should_start_dwell(bus, stops):
    stop = get_next_stop(bus, stops)

    if stop is None:
        return None

    if bus["current_index"] >= stop["routeIndex"]:
        return stop

    return None


def load_observed_stop_dwell_events():
    """
    Reads STOP durations from events_with_real_gps.csv.
    These values come from your own observed ride and are used as realistic dwell references.
    """
    passenger_file = get_active_route_file("passenger_profile_file", REAL_EVENTS_FILE)

    if not passenger_file.exists():
        return []

    try:
        df = normalize_columns(read_csv_auto(passenger_file))
    except Exception as error:
        print(f"WARNING: failed to read dwell events: {error}")
        return []

    required = {"event", "duration", "latitude", "longitude"}
    if not required.issubset(set(df.columns)):
        return []

    df["event"] = df["event"].astype(str).str.strip().str.upper()
    df = df[df["event"] == "STOP"].copy()

    if len(df) == 0:
        return []

    df["duration"] = to_number(df["duration"])
    df["latitude"] = to_number(df["latitude"])
    df["longitude"] = to_number(df["longitude"])
    df = df.dropna(subset=["duration", "latitude", "longitude"])

    events = []

    for _, row in df.iterrows():
        duration = int(max(0, row["duration"]))

        events.append({
            "durationSeconds": duration,
            "latitude": float(row["latitude"]),
            "longitude": float(row["longitude"])
        })

    return events


def attach_observed_dwell_to_stops(route_stops):
    """
    Maps observed STOP durations to the nearest stop in our route_57_stops.csv.
    If a stop has no direct observed duration, we later use activity/load fallback.
    """
    dwell_events = load_observed_stop_dwell_events()

    if not dwell_events:
        print("Observed stop dwell profile: not found, using fallback dwell logic.")
        for stop in route_stops:
            stop["observedDwellSeconds"] = None
        return route_stops

    for stop in route_stops:
        best_event = None
        best_distance = None

        for event in dwell_events:
            distance = distance_meters(
                float(stop["latitude"]),
                float(stop["longitude"]),
                float(event["latitude"]),
                float(event["longitude"])
            )

            if best_distance is None or distance < best_distance:
                best_distance = distance
                best_event = event

        if best_event is not None and best_distance is not None and best_distance <= 450:
            observed = int(best_event["durationSeconds"])
            stop["observedDwellSeconds"] = max(
                MIN_DWELL_SECONDS,
                min(MAX_DWELL_SECONDS, observed)
            )
            stop["observedDwellDistanceMeters"] = float(best_distance)
        else:
            stop["observedDwellSeconds"] = None
            stop["observedDwellDistanceMeters"] = None

    print("\nObserved dwell times mapped to stops:")
    for stop in route_stops:
        if stop["observedDwellSeconds"] is not None:
            print(
                f"{stop['stopSequence']}. {stop['name']} | "
                f"dwell={stop['observedDwellSeconds']}s | "
                f"distance={stop['observedDwellDistanceMeters']:.1f}m"
            )

    return route_stops


def get_max_alighting_for_stop(stop, load_level: str, current_passengers: int) -> int:
    taper_rules = ACTIVE_ROUTE_CONFIG.get("terminal_taper_rules", {})

    if stop.get("isFinalStop"):
        if taper_rules.get("final_stop_allows_full_alighting", True):
            return min(current_passengers, MAX_ALIGHTING_FINAL_STOP)

    if is_scenario_focus_stop(stop.get("name", "")):
        return min(current_passengers, MAX_ALIGHTING_FOCUS_STOP)

    if normalize_load_level(load_level) == "ZERO":
        # Near the route end, allow more people to leave, but not -50 at a normal stop.
        return min(current_passengers, int(taper_rules.get("zero_load_max_alighting", 24)))

    return min(current_passengers, MAX_ALIGHTING_PER_STOP)


def get_max_boarding_for_stop(stop, load_level: str) -> int:
    if stop.get("isFirstStop"):
        return MAX_BOARDING_FIRST_STOP

    if is_scenario_focus_stop(stop.get("name", "")):
        return MAX_BOARDING_FOCUS_STOP

    if normalize_load_level(load_level) == "HIGH":
        return MAX_BOARDING_FOCUS_STOP

    return MAX_BOARDING_PER_STOP


def calculate_dwell_ticks(boarding: int, alighting: int, stop=None, load_level: str = FALLBACK_LOAD_LEVEL) -> int:
    passenger_activity = int(boarding) + int(alighting)

    observed_dwell = None
    if stop is not None:
        observed_dwell = stop.get("observedDwellSeconds")

    # Dwell time must be realistic for the actual boarding/alighting at THIS stop.
    # Your observed dwell values are still used, but they are capped by passenger activity.
    # Example: if only 2 people exit, the bus should not stay 80 seconds.
    if observed_dwell is not None:
        observed_dwell = int(observed_dwell)

        if passenger_activity <= 2:
            activity_cap = 18
        elif passenger_activity <= 6:
            activity_cap = 28
        elif passenger_activity <= 15:
            activity_cap = 45
        else:
            activity_cap = 70

        dwell = min(observed_dwell, activity_cap)

        # Heavy boarding still needs a little more visible dwell time.
        dwell += min(8, passenger_activity // 5)
    else:
        base_by_load = {
            "ZERO": 8,
            "LOW": 14,
            "MID": 22,
            "HIGH": 32
        }
        dwell = base_by_load.get(normalize_load_level(load_level), FALLBACK_DWELL_SECONDS)
        dwell += min(18, passenger_activity // 2)

    return max(MIN_DWELL_SECONDS, min(MAX_DWELL_SECONDS, int(dwell)))


def store_unserved_queue(stop, queue_count: int):
    if queue_count <= 0:
        return

    stop_key = get_stop_key(stop)
    previous_queue = int(UNSERVED_QUEUES_BY_STOP.get(stop_key, 0))
    UNSERVED_QUEUES_BY_STOP[stop_key] = previous_queue + int(queue_count)

    print(
        f"UNSERVED QUEUE stored at {stop['name']} | "
        f"+{queue_count}, total={UNSERVED_QUEUES_BY_STOP[stop_key]}"
    )


def is_support_critical_stop(stop) -> bool:
    stop_name = normalize_text_for_matching(stop.get("name", ""))
    critical_stops = ACTIVE_ROUTE_CONFIG.get("critical_stops", SUPPORT_CRITICAL_STOP_NAMES)

    return any(
        normalize_text_for_matching(critical_name) in stop_name
        for critical_name in critical_stops
    )


def count_active_support_buses() -> int:
    return sum(
        1
        for bus in BUSES
        if bus.get("busRole") == "SUPPORT" and bus.get("active", True)
    )


def find_stop_by_name_contains(route_stops, anchor_name: str):
    wanted = normalize_text_for_matching(anchor_name)

    for stop in route_stops:
        current = normalize_text_for_matching(stop.get("name", ""))

        if wanted in current or current in wanted:
            return stop

    return None


def configure_support_reserve_points(route_stops):
    """
    Convert named reserve anchor stops to route indices.
    This makes support origin understandable in the dashboard and diploma text.
    """
    global SUPPORT_RESERVE_POINTS

    reserve_points = []

    for config in ACTIVE_ROUTE_CONFIG.get("reserve_points", SUPPORT_RESERVE_ANCHOR_STOPS):
        anchor_stop = find_stop_by_name_contains(
            route_stops,
            config["anchorStopName"]
        )

        if anchor_stop is None:
            print(
                f"WARNING: reserve anchor not found: {config['anchorStopName']}"
            )
            continue

        reserve_points.append({
            "reserveName": config["reserveName"],
            "anchorStopName": anchor_stop["name"],
            "routeIndex": int(anchor_stop["routeIndex"]),
            "stopSequence": int(anchor_stop["stopSequence"])
        })

    # sort and de-duplicate by route index
    unique = {}
    for point in reserve_points:
        unique[int(point["routeIndex"])] = point

    SUPPORT_RESERVE_POINTS = [
        unique[index]
        for index in sorted(unique.keys())
    ]

    print("\nNamed support reserve points:")
    if not SUPPORT_RESERVE_POINTS:
        print("  none found, fallback percentage reserves will be used")
    else:
        for point in SUPPORT_RESERVE_POINTS:
            print(
                f"  {point['reserveName']} | "
                f"anchor={point['anchorStopName']} | "
                f"routeIndex={point['routeIndex']}"
            )


def get_support_reserve_candidates() -> list:
    if SUPPORT_RESERVE_POINTS:
        return [int(point["routeIndex"]) for point in SUPPORT_RESERVE_POINTS]

    if ROUTE_POINT_COUNT <= 1:
        return [0]

    candidates = []
    max_index = ROUTE_POINT_COUNT - 1

    for fraction in SUPPORT_RESERVE_FRACTIONS:
        index = int(round(max_index * float(fraction)))
        index = max(0, min(max_index, index))
        candidates.append(index)

    return sorted(set(candidates))


def build_dynamic_nearby_reserve_point(target_route_index: int, target_stop_name: str) -> dict:
    dynamic_index = max(0, int(target_route_index) - SUPPORT_DYNAMIC_LOOKBACK_POINTS)

    return {
        "reserveName": f"Dynamic nearby standby — before {target_stop_name}",
        "anchorStopName": "route segment",
        "routeIndex": int(dynamic_index),
        "stopSequence": None,
        "dynamicReserve": True
    }


def choose_support_reserve_point(target_route_index: int, target_stop_name: str = "") -> dict:
    """
    Choose support origin.

    Priority:
    1. nearest named reserve point upstream;
    2. if that named reserve is too far, use a nearby upstream standby segment;
    3. fallback to route percentage / lookback.
    """
    target_route_index = int(target_route_index)
    target_stop_name = str(target_stop_name or "target stop")
    upstream_limit = target_route_index - SUPPORT_MIN_UPSTREAM_GAP_POINTS

    if SUPPORT_RESERVE_POINTS:
        upstream_points = [
            point
            for point in SUPPORT_RESERVE_POINTS
            if int(point["routeIndex"]) <= upstream_limit
        ]

        if upstream_points:
            selected = max(upstream_points, key=lambda item: int(item["routeIndex"]))
            distance_points = target_route_index - int(selected["routeIndex"])

            if distance_points > SUPPORT_MAX_NAMED_RESERVE_DISTANCE_POINTS:
                return build_dynamic_nearby_reserve_point(
                    target_route_index,
                    target_stop_name
                )

            selected = dict(selected)
            selected["dynamicReserve"] = False
            return selected

    # Fallback to numeric candidates if named reserve points are unavailable.
    candidates = get_support_reserve_candidates()
    upstream_candidates = [
        candidate
        for candidate in candidates
        if candidate <= upstream_limit
    ]

    if upstream_candidates:
        selected_index = max(upstream_candidates)
    else:
        selected_index = max(0, target_route_index - SUPPORT_FALLBACK_LOOKBACK_POINTS)

    distance_points = target_route_index - int(selected_index)

    if distance_points > SUPPORT_MAX_NAMED_RESERVE_DISTANCE_POINTS:
        return build_dynamic_nearby_reserve_point(
            target_route_index,
            target_stop_name
        )

    return {
        "reserveName": "Fallback upstream reserve point",
        "anchorStopName": "route shape",
        "routeIndex": int(selected_index),
        "stopSequence": None,
        "dynamicReserve": False
    }


def describe_support_origin(reserve_point: dict, target_index: int) -> str:
    start_index = int(reserve_point["routeIndex"])
    distance_points = max(0, int(target_index) - start_index)

    return (
        f"{reserve_point['reserveName']} "
        f"({distance_points} route points before target)"
    )


def find_reusable_support_bus_for_stop(stop):
    """
    Reuse an existing Support bus instead of spawning a new one.

    Rules:
    - support bus must be active;
    - support bus must already have completed its first rescue pickup
      and be continuing the route;
    - support bus must still be upstream of the new problem stop;
    - support bus must have available capacity.
    """
    target_route_index = int(stop["routeIndex"])
    candidates = []

    for bus in BUSES:
        if bus.get("busRole") != "SUPPORT":
            continue

        if not bus.get("active", True):
            continue

        if bus.get("state") != STATE_CONTINUING_ROUTE_AFTER_PICKUP:
            continue

        current_index = int(bus.get("current_index", 0))

        if current_index > target_route_index:
            continue

        max_gap_points = ACTIVE_ROUTE_CONFIG.get("support_bus_rules", {}).get(
            "reuse_max_gap_points",
            SUPPORT_REUSE_MAX_GAP_POINTS
        )

        if max_gap_points is not None and (target_route_index - current_index) > int(max_gap_points):
            continue

        trigger_queue = int(get_support_rule("trigger_queue", SUPPORT_BUS_TRIGGER_QUEUE))
        free_space = int(bus.get("capacity", SUPPORT_BUS_CAPACITY)) - int(bus.get("passengerCount", 0))

        if free_space < trigger_queue:
            continue

        candidates.append((current_index, bus))

    if not candidates:
        return None

    # choose the nearest upstream support bus
    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1]


def assign_existing_support_bus_to_stop(support_bus, stop):
    stop_key = get_stop_key(stop)

    support_bus["state"] = STATE_EN_ROUTE_TO_UNSERVED_QUEUE
    support_bus["targetStopSequence"] = int(stop["stopSequence"])
    support_bus["targetStopName"] = stop["name"]
    support_bus["targetRouteIndex"] = int(stop["routeIndex"])
    support_bus["originDescription"] = "reused active support bus already on route"
    support_bus["originReserveName"] = "Active support bus"

    DISPATCHED_SUPPORT_STOPS.add(stop_key)

    print(
        f"REASSIGN {support_bus['busId']} -> {stop['name']} | "
        f"current_idx={support_bus['current_index']}, "
        f"target_idx={stop['routeIndex']}, "
        f"passengers={support_bus['passengerCount']}/{support_bus.get('capacity', SUPPORT_BUS_CAPACITY)}, "
        f"unserved queue={UNSERVED_QUEUES_BY_STOP.get(stop_key, 0)}"
    )

    return support_bus


def dispatch_support_bus_if_needed(stop, requester_bus=None):
    global SUPPORT_BUS_COUNTER

    support_enabled = bool(get_support_rule("enabled", SUPPORT_BUS_ENABLED))
    support_capacity = int(get_support_rule("capacity", SUPPORT_BUS_CAPACITY))
    support_trigger_queue = int(get_support_rule("trigger_queue", SUPPORT_BUS_TRIGGER_QUEUE))
    max_active_support = int(get_support_rule("max_active_support_buses", MAX_ACTIVE_SUPPORT_BUSES))
    support_route_id = str(get_support_rule("support_route_id", SUPPORT_BUS_ROUTE_ID))

    if not support_enabled:
        return None

    if not is_support_critical_stop(stop):
        return None

    requester_role = "MAIN"
    requester_id = "Bus_1"

    if requester_bus is not None:
        requester_role = str(requester_bus.get("busRole", "MAIN"))
        requester_id = str(requester_bus.get("busId", "Bus_1"))

    active_support_count = count_active_support_buses()

    # Important rule:
    # Bus_1 should not spawn many new support buses while support is already active.
    # First try to reuse an active Support bus that is already on the route and still has capacity.
    if requester_role != "SUPPORT" and active_support_count >= 1:
        reusable_support_bus = find_reusable_support_bus_for_stop(stop)

        if reusable_support_bus is not None:
            return assign_existing_support_bus_to_stop(reusable_support_bus, stop)

        print(
            f"SUPPORT DISPATCH SKIPPED at {stop['name']} | "
            f"{requester_id} found queue, but no reusable support bus is available yet"
        )
        return None

    # Support buses may request the next support only if they themselves create unserved demand.
    # Still capped to avoid infinite spawning.
    if active_support_count >= max_active_support:
        print(
            f"SUPPORT DISPATCH SKIPPED at {stop['name']} | "
            f"active support bus limit reached ({max_active_support})"
        )
        return None

    stop_key = get_stop_key(stop)
    queue_count = int(UNSERVED_QUEUES_BY_STOP.get(stop_key, 0))

    if queue_count < support_trigger_queue:
        return None

    if stop_key in DISPATCHED_SUPPORT_STOPS:
        return None

    SUPPORT_BUS_COUNTER += 1
    support_bus_id = f"Support_{SUPPORT_BUS_COUNTER}"

    target_route_index = int(stop["routeIndex"])
    reserve_point = choose_support_reserve_point(target_route_index, stop["name"])
    support_start_route_index = int(reserve_point["routeIndex"])
    support_origin_description = describe_support_origin(
        reserve_point,
        target_route_index
    )

    support_bus = {
        "busId": support_bus_id,
        "routeId": support_route_id,
        "current_index": int(support_start_route_index),
        "passengerScale": 1.0,
        "passengerCount": 0,
        "capacity": support_capacity,
        "busRole": "SUPPORT",
        "active": True,
        "dwellRemaining": 0,
        "nextStopIndex": 0,
        "currentStopName": None,
        "lastBoarding": 0,
        "lastAlighting": 0,
        "lastWaiting": 0,
        "lastLoadLevel": "SUPPORT",
        "targetPassengers": queue_count,
        "state": STATE_EN_ROUTE_TO_UNSERVED_QUEUE,
        "targetStopSequence": int(stop["stopSequence"]),
        "targetStopName": stop["name"],
        "targetRouteIndex": int(stop["routeIndex"]),
        "originRouteIndex": int(support_start_route_index),
        "originDescription": support_origin_description,
        "originReserveName": reserve_point["reserveName"],
        "dynamicReserve": bool(reserve_point.get("dynamicReserve", False))
    }

    BUSES.append(support_bus)
    DISPATCHED_SUPPORT_STOPS.add(stop_key)

    print(
        f"DISPATCH {support_bus_id}: {support_origin_description} -> {stop['name']} | "
        f"requested_by={requester_id}, origin_idx={support_start_route_index}, "
        f"target_idx={target_route_index}, unserved queue={queue_count}"
    )

    return support_bus


def start_support_pickup_at_target(bus, stop):
    stop_key = get_stop_key(stop)
    queue_before = int(UNSERVED_QUEUES_BY_STOP.get(stop_key, 0))
    free_space = max(0, int(bus.get("capacity", SUPPORT_BUS_CAPACITY)) - int(bus["passengerCount"]))

    boarding = min(queue_before, free_space)
    queue_after = max(0, queue_before - boarding)

    UNSERVED_QUEUES_BY_STOP[stop_key] = queue_after

    bus["passengerCount"] = int(bus["passengerCount"]) + boarding
    bus["dwellRemaining"] = calculate_dwell_ticks(
        boarding=boarding,
        alighting=0,
        stop=stop,
        load_level="HIGH"
    )
    bus["currentStopName"] = stop["name"]
    bus["lastBoarding"] = boarding
    bus["lastAlighting"] = 0
    bus["lastWaiting"] = queue_after
    bus["lastLoadLevel"] = "SUPPORT"
    bus["targetPassengers"] = queue_before
    bus["state"] = STATE_BOARDING_UNSERVED_QUEUE
    bus["nextStopIndex"] = int(stop.get("stopIndex", 0)) + 1

    print(
        f"SUPPORT PICKUP {bus['busId']} at {stop['name']} | "
        f"picked +{boarding}, queue before={queue_before}, queue after={queue_after}, "
        f"support passengers={bus['passengerCount']}"
    )


def is_support_residual_service(bus) -> bool:
    return (
        bus.get("busRole") == "SUPPORT"
        and bus.get("state") == STATE_CONTINUING_ROUTE_AFTER_PICKUP
    )


def get_support_residual_boarding_demand(bus, stop, load_level: str, route_index: int) -> int:
    """
    Residual passenger demand for a support bus after it already picked up
    the left-behind queue.

    It is intentionally lower than Bus_1 demand because Bus_1 has already
    served the main passenger flow earlier on this route section.
    """
    load_level = normalize_load_level(load_level)
    stop_name = stop.get("name", "")
    current_passengers = int(bus.get("passengerCount", 0))

    rng = random.Random(
        53000
        + int(route_index) * 17
        + int(stop.get("stopSequence", 0)) * 101
        + len(str(bus.get("busId", ""))) * 23
    )

    if load_level == "ZERO":
        base_min, base_max = 0, 1
    elif load_level == "LOW":
        base_min, base_max = 0, 3
    elif load_level == "MID":
        base_min, base_max = 1, 6
    else:
        base_min, base_max = 2, 10

    demand = rng.randint(base_min, base_max)

    if is_scenario_focus_stop(stop_name):
        demand += rng.randint(1, 5)

    demand = int(round(demand * SUPPORT_RESIDUAL_BOARDING_MULTIPLIER))

    # Once support bus becomes quite loaded, residual demand should slow down.
    if current_passengers >= SUPPORT_SOFT_COMFORT_LOAD:
        demand = int(round(demand * 0.45))

    if current_passengers >= SUPPORT_HARD_DISPATCH_LOAD:
        demand = int(round(demand * 0.20))

    return max(0, demand)


def get_support_residual_alighting(bus, stop, load_level: str, route_index: int) -> int:
    current_passengers = int(bus.get("passengerCount", 0))

    if current_passengers <= 0:
        return 0

    rng = random.Random(
        71000
        + int(route_index) * 19
        + int(stop.get("stopSequence", 0)) * 89
        + current_passengers
    )

    if stop.get("isFinalStop"):
        return min(current_passengers, rng.randint(max(4, current_passengers // 2), current_passengers))

    if is_scenario_focus_stop(stop.get("name", "")):
        max_alighting = min(current_passengers, SUPPORT_FOCUS_MAX_ALIGHTING)
        return rng.randint(1, max_alighting) if max_alighting > 0 else 0

    max_alighting = min(current_passengers, SUPPORT_NORMAL_MAX_ALIGHTING)

    if max_alighting <= 0:
        return 0

    # Not every ordinary stop should have alighting.
    if rng.random() < 0.35:
        return 0

    return rng.randint(0, max_alighting)


def start_support_residual_service_dwell(
    bus,
    stop,
    route_row,
    route_index: int,
    allow_support_dispatch: bool = True
) -> bool:
    """
    Support bus after rescue pickup:
    - continues as additional service;
    - boards/alights gradually;
    - does not immediately become 60/60;
    - can escalate only if it really becomes full and leaves capacity-based queue.
    """
    current_passengers = int(bus["passengerCount"])
    bus_capacity = int(bus.get("capacity", SUPPORT_BUS_CAPACITY))

    load_level = get_route_load_level(route_row)

    alighting = get_support_residual_alighting(
        bus=bus,
        stop=stop,
        load_level=load_level,
        route_index=route_index
    )

    after_alighting = max(0, current_passengers - alighting)
    free_space = max(0, bus_capacity - after_alighting)

    boarding_demand = get_support_residual_boarding_demand(
        bus=bus,
        stop=stop,
        load_level=load_level,
        route_index=route_index
    )

    max_boarding = SUPPORT_FOCUS_MAX_BOARDING if is_scenario_focus_stop(stop.get("name", "")) else SUPPORT_NORMAL_MAX_BOARDING
    boarding = min(boarding_demand, free_space, max_boarding)

    # Unserved queue for support bus means real capacity shortage only.
    route_queue_left = max(0, boarding_demand - free_space)

    # If no passenger exchange happens, do not show an artificial STOP.
    if boarding == 0 and alighting == 0 and route_queue_left == 0:
        bus["nextStopIndex"] += 1
        return False

    final_passengers = after_alighting + boarding

    bus["passengerCount"] = final_passengers
    bus["dwellRemaining"] = calculate_dwell_ticks(
        boarding=boarding,
        alighting=alighting,
        stop=stop,
        load_level=load_level
    )
    bus["currentStopName"] = stop["name"]
    bus["lastBoarding"] = boarding
    bus["lastAlighting"] = alighting
    bus["lastWaiting"] = route_queue_left
    bus["lastLoadLevel"] = f"SUPPORT_{load_level}"
    bus["targetPassengers"] = final_passengers
    bus["nextStopIndex"] += 1
    bus["state"] = STATE_CONTINUING_ROUTE_AFTER_PICKUP

    # Escalation to Support_2 only if support bus itself is near/full and cannot serve demand.
    if (
        route_queue_left > 0
        and allow_support_dispatch
        and is_support_critical_stop(stop)
        and final_passengers >= SUPPORT_HARD_DISPATCH_LOAD
    ):
        store_unserved_queue(stop, route_queue_left)
        dispatch_support_bus_if_needed(stop, requester_bus=bus)

    print(
        f"SUPPORT SERVICE {bus['busId']} at {stop['name']} | "
        f"load={load_level}, residual_demand={boarding_demand}, "
        f"+{boarding} boarded, -{alighting} alighted, "
        f"unserved queue left={route_queue_left}, total={bus['passengerCount']}, "
        f"dwell={bus['dwellRemaining']}s"
    )

    return True


def start_dwell_at_stop(bus, stop, route_row, route_index: int, allow_support_dispatch: bool = True):
    """
    Passenger behavior for Bus_1 and for support buses.

    Bus_1 uses the main real passenger profile.
    Support buses after pickup use residual demand, so they do not fill to 60/60 too quickly.
    """
    if is_support_residual_service(bus):
        return start_support_residual_service_dwell(
            bus=bus,
            stop=stop,
            route_row=route_row,
            route_index=route_index,
            allow_support_dispatch=allow_support_dispatch
        )

    current_passengers = int(bus["passengerCount"])
    bus_capacity = int(bus.get("capacity", BUS_CAPACITY))

    base_target_passengers = get_route_target_passengers(route_row, route_index)
    load_level = get_route_load_level(route_row)
    target_passengers = apply_passenger_scenario(
        base_target_passengers,
        stop,
        load_level,
        route_index
    )

    # Realistic alighting:
    # If target drops sharply, do not let 50 passengers leave at a normal stop.
    if target_passengers < current_passengers:
        desired_alighting = current_passengers - target_passengers + random.randint(0, 2)
    else:
        natural_cap = min(5, current_passengers)
        desired_alighting = random.randint(0, natural_cap) if natural_cap > 0 else 0

    max_alighting = get_max_alighting_for_stop(stop, load_level, current_passengers)
    alighting = min(current_passengers, desired_alighting, max_alighting)

    after_alighting = max(0, current_passengers - alighting)

    # Realistic boarding:
    # Demand may be larger than bus capacity or boarding capacity.
    # The leftover becomes an unserved queue for Route_57.
    desired_onboard = max(0, target_passengers)
    boarding_demand = max(0, desired_onboard - after_alighting)

    free_space = max(0, bus_capacity - after_alighting)
    max_boarding = get_max_boarding_for_stop(stop, load_level)

    # Boarding per stop is realistic and limited by boarding throughput,
    # but unserved queue is created ONLY by capacity shortage.
    # This prevents unrealistic +57 boarding in one ordinary stop.
    boarding = min(boarding_demand, free_space, max_boarding)

    final_passengers = after_alighting + boarding

    # Unserved queue means passengers who could not board because the bus was physically full.
    # It is NOT caused by the per-stop boarding throughput cap.
    route_queue_left = max(0, boarding_demand - free_space)

    bus["passengerCount"] = final_passengers
    bus["dwellRemaining"] = calculate_dwell_ticks(
        boarding=boarding,
        alighting=alighting,
        stop=stop,
        load_level=load_level
    )
    bus["currentStopName"] = stop["name"]
    bus["lastBoarding"] = boarding
    bus["lastAlighting"] = alighting
    bus["lastWaiting"] = route_queue_left
    bus["lastLoadLevel"] = load_level
    bus["targetPassengers"] = target_passengers
    bus["nextStopIndex"] += 1
    bus["state"] = STATE_STOPPING_AT_STOP

    if route_queue_left > 0 and allow_support_dispatch:
        store_unserved_queue(stop, route_queue_left)
        dispatch_support_bus_if_needed(stop, requester_bus=bus)

    print(
        f"STOP {bus['busId']} at {stop['name']} | "
        f"load={load_level}, target={target_passengers}, "
        f"+{boarding} boarded, -{alighting} alighted, "
        f"unserved queue left={route_queue_left}, total={bus['passengerCount']}, "
        f"dwell={bus['dwellRemaining']}s"
    )

    return True


def reset_stop_state_if_needed(bus):
    if bus["dwellRemaining"] == 0:
        if bus.get("busRole") == "SUPPORT" and bus.get("state") == STATE_BOARDING_UNSERVED_QUEUE:
            bus["state"] = STATE_CONTINUING_ROUTE_AFTER_PICKUP
        elif bus.get("state") == STATE_STOPPING_AT_STOP:
            bus["state"] = STATE_IN_SERVICE

        bus["currentStopName"] = None
        bus["lastBoarding"] = 0
        bus["lastAlighting"] = 0
        bus["lastWaiting"] = 0
        bus["lastLoadLevel"] = None
        bus["targetPassengers"] = None
        bus["current_index"] += 1


def handle_support_bus_tick(bus, route_df, route_stops):
    current_index = int(bus["current_index"])

    if current_index >= len(route_df):
        bus["active"] = False
        bus["state"] = STATE_COMPLETED_ROUTE
        return None

    route_row = route_df.iloc[current_index]

    if bus["dwellRemaining"] > 0:
        telemetry = build_stop_telemetry(route_row, bus, current_index)
        bus["dwellRemaining"] -= 1
        reset_stop_state_if_needed(bus)
        return telemetry

    # Before the target stop, support bus does NOT stop.
    # It goes directly to the unserved queue.
    if bus.get("state") == STATE_EN_ROUTE_TO_UNSERVED_QUEUE:
        target_stop = get_stop_by_sequence(route_stops, int(bus["targetStopSequence"]))

        if target_stop is not None and current_index >= int(target_stop["routeIndex"]):
            start_support_pickup_at_target(bus, target_stop)
            telemetry = build_stop_telemetry(route_row, bus, current_index)
            bus["dwellRemaining"] -= 1
            reset_stop_state_if_needed(bus)
            return telemetry

        telemetry = build_moving_telemetry(route_row, bus, current_index, route_df=route_df)
        bus["current_index"] += 1
        return telemetry

    # After picking up the left-behind passengers, support bus becomes an
    # additional in-service bus and stops at the remaining route stops.
    arrived_stop = should_start_dwell(bus, route_stops)

    if arrived_stop is not None:
        did_stop = start_dwell_at_stop(
            bus,
            arrived_stop,
            route_row,
            current_index,
            allow_support_dispatch=True
        )

        if did_stop:
            telemetry = build_stop_telemetry(route_row, bus, current_index)
            bus["dwellRemaining"] -= 1
            reset_stop_state_if_needed(bus)
            return telemetry

        telemetry = build_moving_telemetry(route_row, bus, current_index, route_df=route_df)
        bus["current_index"] += 1
        return telemetry

    telemetry = build_moving_telemetry(route_row, bus, current_index, route_df=route_df)
    bus["current_index"] += 1

    return telemetry


# ==================================================
# TELEMETRY BUILDERS
# ==================================================
def get_current_timestamp():
    return datetime.now().isoformat(timespec="seconds")


def evaluate_reroute_recommendation(
    route_df,
    route_index: int,
    speed: float,
    traffic_level: str,
    event_type: str,
    emergency_event: bool = False
) -> dict:
    """
    Preparation hook for future rerouting.

    The simulator does not invent alternate paths. A reroute can only become
    actionable after an alternative route geometry is configured for the route.
    """
    rules = REROUTE_RULES
    reasons = []

    if route_df is not None and len(route_df) > 0:
        start = int(route_index)
        end = min(len(route_df), start + int(rules["upcoming_window_points"]))
        upcoming = route_df.iloc[start:end]
        high_segments = 0

        for _, point in upcoming.iterrows():
            segment_speed = float(point.get("real_speed", FALLBACK_SPEED_MPS))
            if speed_to_traffic_level(segment_speed) == "HIGH":
                high_segments += 1

        if high_segments >= int(rules["high_traffic_segments_threshold"]):
            reasons.append("CONGESTION_ZONE_AHEAD")

    if float(speed) <= float(rules["average_speed_threshold_mps"]) and traffic_level == "HIGH":
        reasons.append("LOW_AVERAGE_SPEED")

    if event_type == "TRAFFIC" and traffic_level == "HIGH":
        reasons.append("DELAY_RISK_HIGH")

    if emergency_event:
        reasons.append("EMERGENCY_EVENT")

    alternative_exists = bool(rules.get("alternative_route_exists", False))
    recommended = bool(reasons and alternative_exists)

    return {
        "recommended": recommended,
        "status": "READY_FOR_ALTERNATIVE" if recommended else "MONITOR_ONLY",
        "reasonCodes": reasons,
        "alternativeRouteId": None,
        "alternativeRouteExists": alternative_exists
    }


def build_stop_telemetry(route_row, bus, route_index: int):
    return {
        "busId": bus["busId"],
        "routeId": bus["routeId"],
        "timestamp": get_current_timestamp(),
        "latitude": round(float(route_row["latitude"]), 6),
        "longitude": round(float(route_row["longitude"]), 6),
        "speed": 0.0,
        "passengerCount": int(bus["passengerCount"]),
        "doorStatus": "OPEN",
        "trafficLevel": "LOW",
        "eventType": "STOP",
        "eventDuration": int(bus["dwellRemaining"]),
        "routeIndex": int(route_index),
        "currentStopName": bus["currentStopName"],
        "boardingCount": int(bus["lastBoarding"]),
        "alightingCount": int(bus["lastAlighting"]),
        "waitingPassengers": int(bus["lastWaiting"]),
        "dwellRemaining": int(bus["dwellRemaining"]),
        "observedLoadLevel": bus.get("lastLoadLevel"),
        "targetPassengers": bus.get("targetPassengers"),
        "scenarioMode": SCENARIO_MODE,
        "busRole": bus.get("busRole", "MAIN"),
        "capacity": int(bus.get("capacity", BUS_CAPACITY)),
        "state": bus.get("state", STATE_IN_SERVICE),
        "originRouteIndex": bus.get("originRouteIndex"),
        "targetRouteIndex": bus.get("targetRouteIndex"),
        "originDescription": bus.get("originDescription"),
        "originReserveName": bus.get("originReserveName"),
        "dynamicReserve": bus.get("dynamicReserve")
    }


def build_moving_telemetry(route_row, bus, route_index: int, route_df=None):
    base_speed = float(route_row.get("real_speed", FALLBACK_SPEED_MPS))
    speed = adapt_speed_for_bus(base_speed)
    traffic_level = speed_to_traffic_level(speed)
    load_level = get_route_load_level(route_row)
    target_passengers = get_route_target_passengers(route_row, route_index)

    if traffic_level == "HIGH":
        event_type = "TRAFFIC"
    elif speed <= 1.0:
        event_type = "IDLE"
    else:
        event_type = "MOVING"

    reroute_recommendation = evaluate_reroute_recommendation(
        route_df=route_df,
        route_index=route_index,
        speed=speed,
        traffic_level=traffic_level,
        event_type=event_type
    )

    return {
        "busId": bus["busId"],
        "routeId": bus["routeId"],
        "timestamp": get_current_timestamp(),
        "latitude": round(float(route_row["latitude"]), 6),
        "longitude": round(float(route_row["longitude"]), 6),
        "speed": float(speed),
        "passengerCount": int(bus["passengerCount"]),
        "doorStatus": "CLOSED",
        "trafficLevel": traffic_level,
        "eventType": event_type,
        "eventDuration": 0,
        "routeIndex": int(route_index),
        "currentStopName": None,
        "boardingCount": 0,
        "alightingCount": 0,
        "waitingPassengers": 0,
        "dwellRemaining": 0,
        "observedLoadLevel": load_level,
        "targetPassengers": target_passengers,
        "scenarioMode": SCENARIO_MODE,
        "busRole": bus.get("busRole", "MAIN"),
        "capacity": int(bus.get("capacity", BUS_CAPACITY)),
        "state": bus.get("state", STATE_IN_SERVICE),
        "originRouteIndex": bus.get("originRouteIndex"),
        "targetRouteIndex": bus.get("targetRouteIndex"),
        "originDescription": bus.get("originDescription"),
        "originReserveName": bus.get("originReserveName"),
        "dynamicReserve": bus.get("dynamicReserve"),
        "rerouteRecommendation": reroute_recommendation
    }


def publish_telemetry(client, telemetry):
    message = json.dumps(telemetry, ensure_ascii=False)

    for topic in MQTT_TOPICS:
        client.publish(topic, message)

    print(
        f"Sent bus={telemetry['busId']} "
        f"idx={telemetry['routeIndex']} "
        f"speed={telemetry['speed']} "
        f"event={telemetry['eventType']} "
        f"passengers={telemetry['passengerCount']} "
        f"door={telemetry['doorStatus']}"
    )


# ==================================================
# MAIN LOOP
# ==================================================
def main():
    global ROUTE_POINT_COUNT

    print("Project folder:", BASE_DIR)
    print("Active route:", ACTIVE_ROUTE_CONFIG["route_id"])
    print("Route shape file:", get_active_route_file("route_shape_file", SHAPE_FILE))
    print("Stops file:", get_active_route_file("route_stops_file", STOPS_FILE))
    print("Real speed profile file:", get_active_route_file("speed_profile_file", REAL_SPEED_FILE))
    print("Real passenger events file:", get_active_route_file("passenger_profile_file", REAL_EVENTS_FILE))

    route_df = load_route_shape()
    ROUTE_POINT_COUNT = len(route_df)
    stops_df = load_route_stops()

    speed_profile = load_real_speed_profile(len(route_df))
    route_df["real_speed"] = speed_profile

    print(
        "Real speed profile loaded:",
        f"min={round(min(speed_profile) * 3.6, 1)} km/h,",
        f"avg={round((sum(speed_profile) / len(speed_profile)) * 3.6, 1)} km/h,",
        f"max={round(max(speed_profile) * 3.6, 1)} km/h"
    )

    load_levels, passenger_targets = build_passenger_load_profile(route_df)
    route_df["load_level"] = load_levels
    route_df["target_passengers"] = passenger_targets

    print(
        "Passenger load profile loaded:",
        f"start={load_levels[0]},",
        f"middle={load_levels[len(load_levels) // 2]},",
        f"end={load_levels[-1]},",
        f"target min={min(passenger_targets)},",
        f"target avg={round(sum(passenger_targets) / len(passenger_targets), 1)},",
        f"target max={max(passenger_targets)}"
    )

    scenario_config = get_scenario_config()
    print(
        "Scenario mode:",
        SCENARIO_MODE,
        "|",
        scenario_config["description"]
    )

    route_stops = map_stops_to_route(route_df, stops_df)
    route_stops = attach_observed_dwell_to_stops(route_stops)
    configure_support_reserve_points(route_stops)
    initialize_bus_positions(route_stops)
    apply_dispatch_schedule(ACTIVE_ROUTE_CONFIG, route_stops)

    client = mqtt.Client()
    client.connect(BROKER, PORT, 60)
    client.loop_start()

    print("\nNew route-shape simulator started.")
    print(f"Publishing MQTT topics: {', '.join(MQTT_TOPICS)}")
    print("Bus_1 is the main bus.")
    print("Support buses are dispatched only at critical stops with unserved queue.")
    print(f"Max active support buses: {MAX_ACTIVE_SUPPORT_BUSES}")
    print("Unserved queue is created only when passenger demand exceeds free capacity.")
    print("Active support buses can be reused for later unserved queues if they are upstream and have free capacity.")
    print(
        "If the nearest named reserve is too far, a nearby upstream standby segment is used "
        f"({SUPPORT_DYNAMIC_LOOKBACK_POINTS} route points before target)."
    )
    print(
        "Support buses use residual demand after rescue pickup "
        f"(multiplier={SUPPORT_RESIDUAL_BOARDING_MULTIPLIER})."
    )
    print("Bus_1 cannot spawn another support bus while one support intervention is active.")
    print("Support_1 may spawn Support_2 only if Support_1 itself becomes full and leaves queue.")
    print(f"Support reserve route indices: {get_support_reserve_candidates()}")
    print("Old gps_clean_full.csv is used only for speed profile, not for route geometry.")
    print("events_with_real_gps.csv is used only for passenger load profile.")
    print(f"Scenario Engine active: {SCENARIO_MODE}\n")

    simulation_active = True
    simulation_second = 0

    while simulation_active:
        simulation_active = False

        for bus in list(BUSES):
            if bus.get("state") == STATE_WAITING_TO_START:
                simulation_active = True

                if not activate_bus_if_ready(bus, simulation_second):
                    continue

            if not bus.get("active", True):
                continue

            current_index = int(bus["current_index"])

            if current_index >= len(route_df):
                bus["active"] = False
                bus["state"] = STATE_COMPLETED_ROUTE
                continue

            simulation_active = True

            if bus.get("busRole") == "SUPPORT":
                telemetry = handle_support_bus_tick(bus, route_df, route_stops)

                if telemetry is not None:
                    publish_telemetry(client, telemetry)

                continue

            route_row = route_df.iloc[current_index]

            if bus["dwellRemaining"] > 0:
                telemetry = build_stop_telemetry(route_row, bus, current_index)
                bus["dwellRemaining"] -= 1
                reset_stop_state_if_needed(bus)

            else:
                arrived_stop = should_start_dwell(bus, route_stops)

                if arrived_stop is not None:
                    start_dwell_at_stop(bus, arrived_stop, route_row, current_index)
                    telemetry = build_stop_telemetry(route_row, bus, current_index)
                    bus["dwellRemaining"] -= 1
                    reset_stop_state_if_needed(bus)

                else:
                    telemetry = build_moving_telemetry(route_row, bus, current_index, route_df=route_df)
                    bus["current_index"] += 1

            publish_telemetry(client, telemetry)

        time.sleep(PUBLISH_INTERVAL_SECONDS)
        simulation_second += PUBLISH_INTERVAL_SECONDS

    print("Simulation finished: all active buses reached the end of the route.")


if __name__ == "__main__":
    main()
