"""
Utilities for managing initial sensor data in MongoDB.
"""

from typing import Any, Callable, Iterable, Optional, Tuple

LogFn = Callable[[str], None]


def ensure_initial_sensors(
    mongodb_service,
    *,
    log_info: Optional[LogFn] = None,
    log_error: Optional[LogFn] = None,
) -> None:
    """
    Ensure the database has initial sensors; create them when empty.

    Parameters
    ----------
    mongodb_service
        Service that exposes `conectado`, `obtener_sensores` and `crear_sensor`.
    log_info
        Optional function to emit informational messages. Defaults to `print`.
    log_error
        Optional function to emit error messages. Defaults to `print`.
    """

    log_info = log_info or print
    log_error = log_error or print

    if not mongodb_service or not getattr(mongodb_service, "conectado", False):
        log_error("ERROR MongoDB Atlas no disponible para cargar sensores")
        return

    try:
        sensores_existentes = list(mongodb_service.obtener_sensores() or [])
        if sensores_existentes:
            log_info(f"OK Sensores cargados desde MongoDB: {len(sensores_existentes)} sensores")
            return

        log_info("⚠️ No hay sensores en MongoDB, creando sensores básicos...")
        _create_basic_sensors(
            mongodb_service,
            sensores_existentes,
            log_info=log_info,
            log_error=log_error,
        )
    except Exception as exc:  # pylint: disable=broad-except
        log_error(f"ERROR cargando sensores: {exc}")


def _create_basic_sensors(
    mongodb_service,
    sensores_existentes: Iterable[dict],
    *,
    log_info: Optional[LogFn] = None,
    log_error: Optional[LogFn] = None,
) -> None:
    """Insert default sensor records into MongoDB, skipping duplicates."""

    log_info = log_info or print
    log_error = log_error or print

    existing_ids = {
        sensor.get("sensor_id")
        for sensor in sensores_existentes
        if sensor.get("sensor_id")
    }
    existing_location_type = {
        (normalize_location(sensor.get("location")), str(sensor.get("type", "")).strip().lower())
        for sensor in sensores_existentes
    }

    sensores_basicos = _get_default_sensors()
    creados = 0

    try:
        for sensor in sensores_basicos:
            sensor_id = sensor.get("sensor_id")
            tipo_clave = str(sensor.get("type", "")).strip().lower()
            ubicacion_clave = normalize_location(sensor.get("location"))

            if sensor_id in existing_ids:
                log_info(f"ℹ️ Sensor omitido por ID duplicado: {sensor_id}")
                continue

            if (ubicacion_clave, tipo_clave) in existing_location_type:
                log_info(
                    "ℹ️ Sensor omitido por combinación duplicada "
                    f"(ubicación+tipo): {sensor_id}"
                )
                continue

            mongodb_service.crear_sensor(sensor)
            existing_ids.add(sensor_id)
            existing_location_type.add((ubicacion_clave, tipo_clave))
            creados += 1

        if creados:
            log_info(f"OK Sensores básicos creados: {creados} sensores")
        else:
            log_info("ℹ️ No se crearon sensores básicos; todos fueron omitidos por duplicados")
    except Exception as exc:  # pylint: disable=broad-except
        log_error(f"ERROR creando sensores básicos: {exc}")


def _get_default_sensors() -> Iterable[dict]:
    """Return the default set of sensor definitions."""

    return [
        {
            "sensor_id": "SENSOR_BA_001",
            "name": "Sensor Buenos Aires Centro",
            "location": {"city": "Buenos Aires", "country": "Argentina", "zone": "Centro"},
            "type": "Temperatura",
            "status": "activo",
            "description": "Sensor de temperatura en el centro de Buenos Aires",
            "coordinates": {"lat": -34.6037, "lng": -58.3816},
        },
        {
            "sensor_id": "SENSOR_CBA_001",
            "name": "Sensor Córdoba Norte",
            "location": {"city": "Córdoba", "country": "Argentina", "zone": "Norte"},
            "type": "Humedad",
            "status": "activo",
            "description": "Sensor de humedad en el norte de Córdoba",
            "coordinates": {"lat": -31.4201, "lng": -64.1888},
        },
        {
            "sensor_id": "SENSOR_ROS_001",
            "name": "Sensor Rosario Sur",
            "location": {"city": "Rosario", "country": "Argentina", "zone": "Sur"},
            "type": "Ambos",
            "status": "activo",
            "description": "Sensor combinado de temperatura y humedad en Rosario",
            "coordinates": {"lat": -32.9442, "lng": -60.6505},
        },
    ]


def normalize_location(location: Any) -> Tuple[str, str, str]:
    """
    Normalize a location representation into lowercase (city, country, zone).

    Accepts dictionaries with keys `city`, `country`, `zone` or strings in the
    formats "Ciudad, Zona - País" / "Ciudad - País" / "Ciudad, País".
    """

    city = country = zone = ""

    if isinstance(location, dict):
        city = str(location.get("city", "")).strip()
        country = str(location.get("country", "")).strip()
        zone = str(location.get("zone", "")).strip()
    elif isinstance(location, str):
        raw = location.strip()

        if "-" in raw:
            left, right = raw.split("-", 1)
            country = right.strip()
            left = left.strip()
            if "," in left:
                city, zone = [part.strip() for part in left.split(",", 1)]
            else:
                city = left
        elif "," in raw:
            city, country = [part.strip() for part in raw.split(",", 1)]
        else:
            city = raw

    return (city.lower(), country.lower(), zone.lower())

