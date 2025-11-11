from datetime import date, datetime, timedelta
from typing import Optional, Union
import random

import humanize
import uvicorn
from databricks.sdk.config import Config
from fastapi import Path
from pydantic import conint
from pyspark.sql import functions as F
from starlette.middleware.cors import CORSMiddleware

from demo_iot_generated import main
from demo_iot_generated.main import APIContract
from demo_iot_generated.models import (
    Period,
    ObjectDetectionSummaryGetResponse,
    ObjectDetectionRecentGetResponse,
    ObjectDetectionHourlyGetResponse,
    LicensePlatesStatsGetResponse,
    LicensePlatesRecentGetResponse,
    LicensePlatesDistributionGetResponse,
    Interval,
    DevicesDeviceIdHistoryGetResponse,
    DevicesDeviceIdGetResponse,
    DevicesStatsGetResponse,
    DevicesGetResponse,
    SortDirection,
    ApiSearchGetResponse,
    AlertsStatsGetResponse,
    Type,
    AlertsGetResponse,
    AiChatPostRequest,
    AiChatPostResponse,
    Alert,
    Device,
    DeviceStats,
    TemperatureDataPoint,
    StateDistribution,
    RecentPlate,
    StateStats,
    ObjectDetection,
    HourlyDetection,
    RecentDetection,
    Data, Error, Status,
)
from reggie_tools import configs, clients


class APIImplementation(APIContract):
    """Dummy API implementation with generated responses."""

    def __init__(self, config: Config):
        self.config = config
        self.spark = clients.spark(config)

    def send_chat_message(self, body: AiChatPostRequest) -> Union[AiChatPostResponse, Error]:
        resp = AiChatPostResponse(
            success=True,
            data={
                "response": f"Received your message: '{body.message}'. Average temperature is 75.4°F.",
                "confidence": 0.95,
                "timestamp": datetime.utcnow(),
            },
        )
        return resp

    def get_alerts(self, type: Optional[Type] = None, limit: Optional[conint(ge=1, le=100)] = 50) -> Union[
        AlertsGetResponse, Error]:
        alert_types = ["critical", "warning", "info"]
        devices = ["Refrigerator", "HVAC", "Pump", "Camera"]
        locations = ["Atlanta, GA", "Tampa, FL", "Dallas, TX", "Orlando, FL"]

        alerts: list[Alert] = []
        for i in range(min(limit, 10)):
            alert = Alert(
                id=f"RT-{i:03d}-{random.choice(alert_types)}",
                type=random.choice(list(Type)),
                device=random.choice(devices),
                location=random.choice(locations),
                message=f"Temperature at {random.uniform(70, 100):.1f}°F",
                time=f"{random.randint(5, 120)} sec ago",
                action=random.choice(
                    [
                        "Check HVAC system immediately",
                        "Inspect refrigeration unit",
                        "Verify power connection",
                        "Review recent logs",
                    ]
                ),
                timestamp=datetime.utcnow() - timedelta(seconds=random.randint(0, 300)),
            )
            alerts.append(alert)
        return AlertsGetResponse(success=True, data=alerts)

    def get_alert_stats(self) -> Union[AlertsStatsGetResponse, Error]:
        data = Data(
            criticalCount=3,
            warningCount=4,
            infoCount=5,
            totalCount=12,
        )
        return AlertsStatsGetResponse(success=True, data=data)

    def search_data(self, q: str, limit: Optional[conint(ge=1, le=1000)] = 50, offset: Optional[conint(ge=0)] = 0,
                    sort_column: Optional[str] = None, sort_direction: Optional[SortDirection] = 'asc') -> Union[
        ApiSearchGetResponse, Error]:
        rows = [
            {"device_id": f"RT-ATL-{i:03d}", "location": "Atlanta, GA", "temperature": random.uniform(70, 95),
             "status": random.choice(["normal", "warning", "critical"])}
            for i in range(10)
        ]
        return ApiSearchGetResponse(columns=["device_id", "location", "temperature", "status"], data=rows,
                                    total=len(rows), limit=limit, offset=offset, hasMore=False)

    def get_devices(self) -> Union[DevicesGetResponse, Error]:
        devices = [
            Device(
                id=f"RT-ATL-{i:03d}",
                name=f"Store-{i}",
                location=random.choice(["Atlanta, GA", "Tampa, FL", "Dallas, TX"]),
                currentTemp=random.uniform(70, 95),
                status=random.choice(list(Status)),
                lastUpdate=f"{random.randint(1, 60)} min ago",
                lastUpdateTimestamp=datetime.utcnow() - timedelta(minutes=random.randint(0, 60)),
            )
            for i in range(6)
        ]
        return DevicesGetResponse(success=True, data=devices)

    def get_device_stats(self) -> Union[DevicesStatsGetResponse, Error]:
        stats = DeviceStats(
            normalCount=4,
            warningCount=1,
            criticalCount=1,
            avgTemp="75.4",
            totalDevices=6,
        )
        return DevicesStatsGetResponse(success=True, data=stats)

    def get_device_by_id(self, device_id: str = Path(..., alias='deviceId')) -> Union[
        DevicesDeviceIdGetResponse, Error]:
        device = Device(
            id=device_id,
            name="Store-001",
            location="Atlanta, GA",
            currentTemp=72.4,
            status=Status.normal,
            lastUpdate="2 min ago",
            lastUpdateTimestamp=datetime.utcnow(),
        )
        return DevicesDeviceIdGetResponse(success=True, data=device)

    def get_device_history(self, device_id: str = Path(..., alias='deviceId'),
                           hours: Optional[conint(ge=1, le=168)] = 24, interval: Optional[Interval] = 60) -> Union[
        DevicesDeviceIdHistoryGetResponse, Error]:
        now = datetime.utcnow()
        history = [
            TemperatureDataPoint(
                time=(now - timedelta(hours=i)).strftime("%H:%M"),
                temperature=random.uniform(50, 70),
                humidity=random.uniform(30, 70),
                timestamp=now - timedelta(hours=i),
            )
            for i in range(10)
        ]
        return DevicesDeviceIdHistoryGetResponse(success=True, data=history)

    def get_license_plate_distribution(self, period: Optional[Period] = 'today') -> Union[
        LicensePlatesDistributionGetResponse, Error]:
        states = [
            StateDistribution(
                state=code,
                name=name,
                count=random.randint(100, 500),
                percentage=random.uniform(5, 25),
                color=random.choice(["#dc2626", "#16a34a", "#2563eb"]),
            )
            for code, name in [("GA", "Georgia"), ("FL", "Florida"), ("TX", "Texas")]
        ]
        return LicensePlatesDistributionGetResponse(success=True, data=states)

    def get_recent_plates(self, limit: Optional[conint(ge=1, le=100)] = 20) -> Union[
        LicensePlatesRecentGetResponse, Error]:
        plates = [
            RecentPlate(
                id=i,
                state=random.choice(["GA", "FL", "TX"]),
                plateNumber=f"{random.choice(['ABC', 'XYZ', 'JKL'])}***",
                location="Store",
                time=f"{random.randint(5, 120)} sec ago",
                confidence=random.randint(90, 100),
                timestamp=datetime.utcnow() - timedelta(seconds=random.randint(0, 300)),
            )
            for i in range(limit)
        ]
        return LicensePlatesRecentGetResponse(success=True, data=plates)

    def get_license_plate_stats(self) -> Union[LicensePlatesStatsGetResponse, Error]:
        stats = StateStats(totalDetected=1200, uniqueStates=24, averagePerHour=50, trend="+15%")
        return LicensePlatesStatsGetResponse(success=True, data=stats)

    def get_hourly_detections(self, date: Optional[date] = None) -> Union[ObjectDetectionHourlyGetResponse, Error]:
        detections = [
            HourlyDetection(hour=f"{i:02d}:00", count=random.randint(50, 200),
                            timestamp=datetime.utcnow() - timedelta(hours=i))
            for i in range(10)
        ]
        return ObjectDetectionHourlyGetResponse(success=True, data=detections)

    def get_recent_detections(self, limit: Optional[conint(ge=1, le=100)] = 20) -> Union[
        ObjectDetectionRecentGetResponse, Error]:
        detections_df = (
            self.spark.table("reggie_pierce.iot_ingest.detections")
            .where(F.col("store_id") != 1)
            .where(F.col("label").isNotNull())
            .orderBy(F.col("timestamp").desc())
        ).limit(limit)
        data = []
        for idx, detection in enumerate(detections_df.collect()):
            data.append(RecentDetection(
                id=idx,
                type=detection.label,
                location="Store",
                time=humanize.naturaltime(datetime.utcnow() - detection.timestamp),
                confidence=int(detection.score * 100) if detection.score < 1 else int(detection.scoreg),
                timestamp=detection.timestamp,
            ))
        return ObjectDetectionRecentGetResponse(success=True, data=data)

    def get_object_detection_summary(self, period: Optional[Period] = 'today') -> Union[
        ObjectDetectionSummaryGetResponse, Error]:
        objects = [
            ObjectDetection(object="Vehicles", count=1247, trend="+12%", icon="Car", color="#3b82f6"),
            ObjectDetection(object="People", count=743, trend="-3%", icon="User", color="#22c55e"),
        ]
        return ObjectDetectionSummaryGetResponse(success=True, data=objects)


app = main.create_app(APIImplementation(configs.get()))
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # or specific domains: ["https://example.com"]
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
