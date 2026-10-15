"""Геометрия: расстояние по прямой между координатами (гаверсинус)."""
from __future__ import annotations

import math

EARTH_RADIUS_M = 6_371_008.8


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(min(1.0, math.sqrt(a)))


def offset_north(lat: float, lon: float, meters: float) -> tuple[float, float]:
    """Точка в `meters` метрах к северу (на меридиане гаверсинус даёт точное расстояние)."""
    return lat + math.degrees(meters / EARTH_RADIUS_M), lon
