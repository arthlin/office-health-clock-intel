# core/weather_service.py — 當地天氣資料抓取與快取

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import json
import math
import os
import platform
import subprocess
import threading
import urllib.parse
import urllib.request


@dataclass(frozen=True)
class WeatherSnapshot:
    location: str
    icon: str
    temperature_c: float | None
    wind_kmh: float | None
    rain_mm: float | None
    description: str
    updated_at: datetime


class WeatherService:
    """Fetches local weather from Open-Meteo without an API key."""

    def __init__(self, refresh_ttl_minutes: int = 120):
        self._refresh_ttl = timedelta(minutes=refresh_ttl_minutes)
        self._lock = threading.Lock()
        self._snapshot: WeatherSnapshot | None = None
        self._last_refresh: datetime | None = None
        self._location_cache: dict[str, object] | None = None

    def get_snapshot(self, force: bool = False) -> WeatherSnapshot:
        now = datetime.now()
        with self._lock:
            if not force and self._snapshot and self._last_refresh:
                if now - self._last_refresh < self._refresh_ttl:
                    return self._snapshot

        snapshot = self._refresh()
        with self._lock:
            self._snapshot = snapshot
            self._last_refresh = now
        return snapshot

    def refresh_async(self, callback=None) -> None:
        def worker():
            snapshot = self.get_snapshot(force=True)
            if callback:
                callback(snapshot)

        threading.Thread(target=worker, daemon=True).start()

    def _refresh(self) -> WeatherSnapshot:
        location = self._detect_location()
        if location is None:
            return WeatherSnapshot(
                location="--",
                icon="⛅",
                temperature_c=None,
                wind_kmh=None,
                rain_mm=None,
                description="無法取得",
                updated_at=datetime.now(),
            )

        try:
            return self._fetch_weather(location)
        except Exception:
            pass
        # 備援：嘗試 wttr.in
        try:
            return self._fetch_weather_wttr(location)
        except Exception:
            pass
        return WeatherSnapshot(
            location=str(location.get("name") or "--"),
            icon="⛅",
            temperature_c=None,
            wind_kmh=None,
            rain_mm=None,
            description="無法取得",
            updated_at=datetime.now(),
        )

    def _detect_location(self) -> dict[str, object] | None:
        location = self._detect_location_via_ip()
        if location:
            self._location_cache = location
            return location

        location = self._detect_location_via_timezone()
        if location:
            self._location_cache = location
            return location

        return self._location_cache

    def _detect_location_via_ip(self) -> dict[str, object] | None:
        urls = [
            "http://ip-api.com/json/",
            "https://ipapi.co/json/",
            "https://ipwho.is/",
        ]
        for url in urls:
            try:
                payload = self._http_get_json(url, timeout=5)
                if not payload:
                    continue

                if payload.get("success") is False:
                    continue

                lat = payload.get("latitude") or payload.get("lat")
                lon = payload.get("longitude") or payload.get("lon") or payload.get("lng")
                if lat is None or lon is None:
                    continue

                city = str(payload.get("city") or "")
                region = str(payload.get("regionName") or payload.get("region") or "")
                district = str(payload.get("district") or "")
                country = str(payload.get("country") or "")
                # 英轉中對照表（縣市）
                region_cn = {
                    "New Taipei City": "新北市", "Taipei City": "台北市", "Taichung City": "台中市",
                    "Kaohsiung City": "高雄市", "Tainan City": "台南市", "Taoyuan City": "桃園市",
                    "Keelung City": "基隆市", "Hsinchu City": "新竹市", "Chiayi City": "嘉義市",
                    "Hsinchu County": "新竹縣", "Miaoli County": "苗栗縣", "Changhua County": "彰化縣",
                    "Nantou County": "南投縣", "Yunlin County": "雲林縣", "Chiayi County": "嘉義縣",
                    "Pingtung County": "屏東縣", "Yilan County": "宜蘭縣", "Hualien County": "花蓮縣",
                    "Taitung County": "台東縣", "Penghu County": "澎湖縣",
                }.get(region, region)
                # 英轉中對照表（鄉鎮市區）— ip-api.com 的 city 欄位
                district_cn = {
                    "Banqiao": "板橋區", "Zhonghe": "中和區", "Yonghe": "永和區", "Xindian": "新店區",
                    "Xizhi": "汐止區", "Shulin": "樹林區", "Tucheng": "土城區", "Sanchong": "三重區",
                    "Luzhou": "蘆洲區", "Wugu": "五股區", "Taishan": "泰山區", "Linkou": "林口區",
                    "Shenkeng": "深坑區", "Shiding": "石碇區", "Pinglin": "平溪區", "Sanxia": "三峽區",
                    "Yingge": "鶯歌區", "Danshui": "淡水區", "Bali": "八里區", "Wanli": "萬里區",
                    "Jinshan": "金山區", "Gongliao": "貢寮區", "Shuangxi": "雙溪區", "Ruifang": "瑞芳區",
                    "Pingxi": "平溪區", "Taipei": "台北市", "Taichung": "台中市", "Kaohsiung": "高雄市",
                    "Tainan": "台南市", "Taoyuan": "桃園市", "Hsinchu": "新竹市", "Chiayi": "嘉義市",
                    "Keelung": "基隆市",
                }.get(city, "")
                if district_cn:
                    name = f"{region_cn}{district_cn}"
                elif district:
                    name = f"{region_cn}{district}"
                else:
                    name = region_cn

                timezone = payload.get("timezone") or "auto"
                if isinstance(timezone, dict):
                    timezone = timezone.get("id") or "auto"

                # 嘗試用 Nominatim 取得更精確的中文行政區
                precise = self._reverse_geocode(float(lat), float(lon))
                if precise:
                    name = precise

                return {
                    "name": name,
                    "lat": float(lat),
                    "lon": float(lon),
                    "timezone": timezone,
                }
            except Exception:
                continue
        return None

    @staticmethod
    def _reverse_geocode(lat: float, lon: float) -> str | None:
        """Nominatim 反向地理編碼，取得中文縣市＋行政區。"""
        import urllib.request, urllib.error, json, ssl
        url = (
            f"https://nominatim.openstreetmap.org/reverse"
            f"?lat={lat}&lon={lon}&format=json&accept-language=zh-TW"
        )
        req = urllib.request.Request(url, headers={"User-Agent": "office-health-clock/1.0"})
        for ctx in (None, ssl._create_unverified_context()):
            try:
                kw = {"timeout": 5} if ctx is None else {"timeout": 5, "context": ctx}
                with urllib.request.urlopen(req, **kw) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                addr = data.get("address", {})
                city = (
                    addr.get("city")
                    or addr.get("county")
                    or addr.get("state")
                    or ""
                )
                district = (
                    addr.get("city_district")
                    or addr.get("suburb")
                    or addr.get("town")
                    or addr.get("village")
                    or ""
                )
                if city and district:
                    return f"{city}{district}"
                return city or None
            except Exception:
                continue
        return None

    def _detect_location_via_timezone(self) -> dict[str, object] | None:
        tz_name = None
        if platform.system() == "Windows":
            try:
                result = subprocess.run(
                    ["tzutil", "/g"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                    creationflags=subprocess.CREATE_NO_WINDOW,
                )
                tz_name = result.stdout.strip() or None
            except Exception:
                tz_name = None

        if not tz_name:
            try:
                tz_name = datetime.now().astimezone().tzname()
            except Exception:
                tz_name = None

        if not tz_name:
            return None

        mapped = self._timezone_map().get(tz_name)
        if mapped:
            return mapped

        offset_hours = self._offset_hours()
        return self._offset_map().get(offset_hours)

    @staticmethod
    def _offset_hours() -> int | None:
        try:
            offset = datetime.now().astimezone().utcoffset()
            if offset is None:
                return None
            return int(offset.total_seconds() // 3600)
        except Exception:
            return None

    @staticmethod
    def _timezone_map() -> dict[str, dict[str, object]]:
        return {
            "Taipei Standard Time": {"name": "台北", "lat": 25.0330, "lon": 121.5654, "timezone": "Asia/Taipei"},
            "China Standard Time": {"name": "上海", "lat": 31.2304, "lon": 121.4737, "timezone": "Asia/Shanghai"},
            "Tokyo Standard Time": {"name": "東京", "lat": 35.6762, "lon": 139.6503, "timezone": "Asia/Tokyo"},
            "Korea Standard Time": {"name": "首爾", "lat": 37.5665, "lon": 126.9780, "timezone": "Asia/Seoul"},
            "Singapore Standard Time": {"name": "新加坡", "lat": 1.3521, "lon": 103.8198, "timezone": "Asia/Singapore"},
            "SE Asia Standard Time": {"name": "曼谷", "lat": 13.7563, "lon": 100.5018, "timezone": "Asia/Bangkok"},
            "UTC": {"name": "UTC", "lat": 0.0, "lon": 0.0, "timezone": "UTC"},
        }

    @staticmethod
    def _offset_map() -> dict[int, dict[str, object]]:
        return {
            8: {"name": "台北", "lat": 25.0330, "lon": 121.5654, "timezone": "Asia/Taipei"},
            9: {"name": "東京", "lat": 35.6762, "lon": 139.6503, "timezone": "Asia/Tokyo"},
            7: {"name": "曼谷", "lat": 13.7563, "lon": 100.5018, "timezone": "Asia/Bangkok"},
            1: {"name": "倫敦", "lat": 51.5072, "lon": -0.1276, "timezone": "Europe/London"},
            -5: {"name": "紐約", "lat": 40.7128, "lon": -74.0060, "timezone": "America/New_York"},
            -8: {"name": "洛杉磯", "lat": 34.0522, "lon": -118.2437, "timezone": "America/Los_Angeles"},
        }

    @staticmethod
    def _http_get_json(url: str, timeout: int = 5) -> dict | None:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = resp.read().decode("utf-8")
            return json.loads(data)
        except urllib.error.URLError:
            # SSL/TLS 連線問題時改用未驗證的 context 重試
            try:
                import ssl
                ctx = ssl._create_unverified_context()
                with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
                    data = resp.read().decode("utf-8")
                return json.loads(data)
            except Exception:
                return None

    def _fetch_weather(self, location: dict[str, object]) -> WeatherSnapshot:
        lat = float(location["lat"])
        lon = float(location["lon"])
        timezone = str(location.get("timezone") or "auto")
        params = {
            "latitude": f"{lat:.4f}",
            "longitude": f"{lon:.4f}",
            "current": "temperature_2m,weather_code,wind_speed_10m,precipitation",
            "timezone": timezone,
            "temperature_unit": "celsius",
        }
        url = "https://api.open-meteo.com/v1/forecast?" + urllib.parse.urlencode(params)
        payload = self._http_get_json(url, timeout=8)
        if not payload:
            raise RuntimeError("open-meteo unreachable")
        current = payload.get("current", {})

        temp = current.get("temperature_2m")
        wind = current.get("wind_speed_10m")
        rain = current.get("precipitation")
        code = int(current.get("weather_code", 0) or 0)
        icon, desc = self._weather_code_to_icon(code)

        return WeatherSnapshot(
            location=str(location.get("name") or "當地"),
            icon=icon,
            temperature_c=float(temp) if temp is not None else None,
            wind_kmh=float(wind) if wind is not None else None,
            rain_mm=float(rain) if rain is not None else None,
            description=desc,
            updated_at=datetime.now(),
        )

    def _fetch_weather_wttr(self, location: dict[str, object]) -> WeatherSnapshot:
        lat = float(location["lat"])
        lon = float(location["lon"])
        for scheme in ["https", "http"]:
            try:
                url = f"{scheme}://wttr.in/{lat},{lon}?format=j1"
                req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
                try:
                    with urllib.request.urlopen(req, timeout=8) as resp:
                        data = json.loads(resp.read().decode("utf-8"))
                except Exception:
                    import ssl
                    ctx = ssl._create_unverified_context()
                    with urllib.request.urlopen(req, timeout=8, context=ctx) as resp:
                        data = json.loads(resp.read().decode("utf-8"))
                cc = data.get("current_condition", [{}])[0]
                temp = cc.get("temp_C")
                wind = cc.get("windspeedKmph")
                rain = cc.get("precipMM")
                desc = (cc.get("weatherDesc") or [{}])[0].get("value", "")
                return WeatherSnapshot(
                    location=str(location.get("name") or "當地"),
                    icon="🌤",
                    temperature_c=float(temp) if temp is not None else None,
                    wind_kmh=float(wind) if wind is not None else None,
                    rain_mm=float(rain) if rain is not None else None,
                    description=desc or "天氣",
                    updated_at=datetime.now(),
                )
            except Exception:
                continue
        raise RuntimeError("wttr.in unreachable")

    @staticmethod
    def _weather_code_to_icon(code: int) -> tuple[str, str]:
        if code == 0:
            return "☀", "晴朗"
        if code in (1, 2):
            return "🌤", "多雲"
        if code == 3:
            return "☁", "陰天"
        if code in (45, 48):
            return "🌫", "霧"
        if code in (51, 53, 55, 56, 57):
            return "🌦", "毛毛雨"
        if code in (61, 63, 65, 66, 67):
            return "🌧", "下雨"
        if code in (71, 73, 75, 77):
            return "❄", "下雪"
        if code in (80, 81, 82):
            return "🌦", "陣雨"
        if code in (95, 96, 99):
            return "⛈", "雷雨"
        return "⛅", "天氣"
