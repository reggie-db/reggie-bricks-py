import os
import random
import re
import time
from datetime import date, datetime, timedelta
from typing import Callable, Optional

import humanize
from databricks.sdk.config import Config
from demo_iot_generated.main import APIContract
from demo_iot_generated.models import (
    AiChatPostRequest,
    AiChatPostResponse,
    Alert,
    AlertsGetResponse,
    AlertsStatsGetResponse,
    ApiSearchGetResponse,
    Data,
    Device,
    DevicesDeviceIdGetResponse,
    DevicesDeviceIdHistoryGetResponse,
    DevicesGetResponse,
    DevicesStatsGetResponse,
    DeviceStats,
    Error,
    HourlyDetection,
    Interval,
    LicensePlatesDistributionGetResponse,
    LicensePlatesRecentGetResponse,
    LicensePlatesStatsGetResponse,
    ObjectDetection,
    ObjectDetectionHourlyGetResponse,
    ObjectDetectionRecentGetResponse,
    ObjectDetectionSummaryGetResponse,
    Period,
    RecentDetection,
    RecentPlate,
    SortDirection,
    StateDistribution,
    StateStats,
    Status,
    TemperatureDataPoint,
    Type,
)
from fastapi import Path, Request
from pydantic import conint
from pyspark.sql import functions as F
from reggie_concurio import caches
from reggie_core import logs, objects, paths
from reggie_tools import catalogs, clients, genie

LOG = logs.logger(__file__)


class APIImplementation(APIContract):
    """Dummy API implementation with generated responses."""

    def __init__(
        self, config: Config, current_request: Callable[[], Request | None]
    ) -> None:
        self.config = config
        self.current_request = current_request
        self.spark = clients.spark(config)
        self.genie_service = genie.Service(
            clients.workspace_client(config),
            os.getenv("GENIE_SPACE_ID", "01f09d59bdff163e88db9bc395a1e08e"),
        )
        self.detection_table_name = str(
            catalogs.catalog_schema_table("detections", self.spark)
        )
        self.query_cache = caches.DiskCache(
            paths.temp_dir() / f"{__class__.__name__}_v2"
        )

    def send_chat_message(self, body: AiChatPostRequest) -> AiChatPostResponse | Error:
        resp = AiChatPostResponse(
            success=True,
            data={
                "response": f"Received your message: '{body.message}'. Average temperature is 75.4°F.",
                "confidence": 0.95,
                "timestamp": datetime.utcnow(),
            },
        )
        return resp

    def get_alerts(
        self, type: Optional[Type] = None, limit: Optional[conint(ge=1, le=100)] = 50
    ) -> AlertsGetResponse | Error:
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

    def get_alert_stats(self) -> AlertsStatsGetResponse | Error:
        data = Data(
            criticalCount=3,
            warningCount=4,
            infoCount=5,
            totalCount=12,
        )
        return AlertsStatsGetResponse(success=True, data=data)

    def search_data(
        self,
        q: str,
        limit: Optional[conint(ge=1, le=1000)] = 50,
        offset: Optional[conint(ge=0)] = 0,
        sort_column: Optional[str] = None,
        sort_direction: Optional[SortDirection] = "asc",
    ) -> ApiSearchGetResponse | Error:
        def _conversation_id():
            request = self.current_request()
            session_id = (
                request.headers.get("X-REACT-SESSION-ID", None) if request else None
            )
            LOG.info("session_id: %s", session_id)

            def _load_conversation_id():
                LOG.info("loading conversation id - session_id:%s", session_id)
                conv_id = self.genie_service.create_conversation(
                    "User search on detection data"
                ).conversation_id
                return conv_id

            if session_id:
                return self.query_cache.get_or_load(
                    objects.hash(["conversation_id", session_id]).hexdigest(),
                    _load_conversation_id,
                    expire=60 * 60 * 6,
                ).value
            else:
                return _load_conversation_id()

        def _load_sql():
            conv_id = _conversation_id()
            LOG.info("loading sql - conversation_id:%s", conv_id)

            sql_query = None
            description = None
            sql_query_found = None

            for msg in self.genie_service.chat(conv_id, q):
                LOG.info("genie msg: %s", msg)
                for msg_query in msg.queries():
                    sql_query = re.sub(r"\s*;\s*$", "", msg_query).strip() or sql_query
                    if sql_query and not sql_query_found:
                        sql_query_found = time.time()
                for msg_description in msg.descriptions():
                    description = msg_description
                if (sql_query and description) or (
                    sql_query_found and (time.time() - sql_query_found > 3)
                ):
                    break
            return sql_query, description

        sql, description = self.query_cache.get_or_load(
            objects.hash(["sql_description", q]).hexdigest(), _load_sql
        ).value
        LOG.info(f"sql: {sql}, description: {description}")
        if not sql:
            columns, rows, total = [], [], 0
        else:

            def _load_total():
                clean_sql = re.sub(r"\s*;\s*$", "", sql)
                count_sql = f"SELECT COUNT(*) AS total FROM ({clean_sql}) AS subq"
                return self.spark.sql(count_sql).collect()[0]["total"]

            total = self.query_cache.get_or_load(
                objects.hash(["total", sql]).hexdigest(), _load_total, expire=20
            ).value

            # Add sorting if requested
            if sort_column:
                direction = (
                    "ASC"
                    if sort_direction is not None
                    and str(sort_direction).lower() == "asc"
                    else "DESC"
                )
                sql = f"SELECT * FROM ({sql}) AS subq ORDER BY `{sort_column}` {direction}"
            sql_df = self.spark.sql(sql)
            if sort_column:
                sort_col = F.col(sort_column)
                if SortDirection.desc == sort_direction:
                    sort_col = sort_col.desc_nulls_last()
                else:
                    sort_col = sort_col.asc_nulls_last()
                sql_df = sql_df.orderBy(sort_col)
            rows_df = sql_df.offset(offset).limit(limit)
            columns = rows_df.columns
            rows = [row.asDict() for row in rows_df.collect()]

        LOG.info(f"Returning {len(rows)} rows")
        return ApiSearchGetResponse(
            columns=columns,
            data=rows,
            sql=sql,
            description=description,
            total=total,
            limit=limit,
            offset=offset,
            hasMore=(offset + len(rows)) < total,
        )

    def get_devices(self) -> DevicesGetResponse | Error:
        devices = [
            Device(
                id=f"RT-ATL-{i:03d}",
                name=f"Store-{i}",
                location=random.choice(["Atlanta, GA", "Tampa, FL", "Dallas, TX"]),
                currentTemp=random.uniform(70, 95),
                status=random.choice(list(Status)),
                lastUpdate=f"{random.randint(1, 60)} min ago",
                lastUpdateTimestamp=datetime.utcnow()
                - timedelta(minutes=random.randint(0, 60)),
            )
            for i in range(6)
        ]
        return DevicesGetResponse(success=True, data=devices)

    def get_device_stats(self) -> DevicesStatsGetResponse | Error:
        stats = DeviceStats(
            normalCount=4,
            warningCount=1,
            criticalCount=1,
            avgTemp="75.4",
            totalDevices=6,
        )
        return DevicesStatsGetResponse(success=True, data=stats)

    def get_device_by_id(
        self, device_id: str = Path(..., alias="deviceId")
    ) -> DevicesDeviceIdGetResponse | Error:
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

    def get_device_history(
        self,
        device_id: str = Path(..., alias="deviceId"),
        hours: Optional[conint(ge=1, le=168)] = 24,
        interval: Optional[Interval] = 60,
    ) -> DevicesDeviceIdHistoryGetResponse | Error:
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

    def get_license_plate_distribution(
        self, period: Optional[Period] = "today"
    ) -> LicensePlatesDistributionGetResponse | Error:
        states = [
            StateDistribution(
                state=code,
                name=name,
                count=random.randint(100, 500),
                percentage=round(random.uniform(5, 25), 2),
                color=random.choice(["#dc2626", "#16a34a", "#2563eb"]),
            )
            for code, name in [("GA", "Georgia"), ("FL", "Florida"), ("TX", "Texas")]
        ]
        return LicensePlatesDistributionGetResponse(success=True, data=states)

    def get_recent_plates(
        self, limit: Optional[conint(ge=1, le=100)] = 20
    ) -> LicensePlatesRecentGetResponse | Error:
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

    def get_license_plate_stats(self) -> LicensePlatesStatsGetResponse | Error:
        stats = StateStats(
            totalDetected=1200, uniqueStates=24, averagePerHour=50, trend="+15%"
        )
        return LicensePlatesStatsGetResponse(success=True, data=stats)

    def get_hourly_detections(
        self, date: Optional[date] = None
    ) -> ObjectDetectionHourlyGetResponse | Error:
        detections = [
            HourlyDetection(
                hour=f"{i:02d}:00",
                count=random.randint(50, 200),
                timestamp=datetime.utcnow() - timedelta(hours=i),
            )
            for i in range(10)
        ]
        return ObjectDetectionHourlyGetResponse(success=True, data=detections)

    def get_recent_detections(
        self, limit: Optional[conint(ge=1, le=100)] = 20
    ) -> ObjectDetectionRecentGetResponse | Error:
        detections_df = (
            self.spark.table(str(catalogs.catalog_schema_table("detections")))
            .where(F.col("store_id") != 1)
            .where(F.col("label").isNotNull())
            .orderBy(F.col("timestamp").desc())
            .dropDuplicates(["label"])
            .limit(limit)
        )
        data = []
        for idx, detection in enumerate(detections_df.collect()):
            data.append(
                RecentDetection(
                    id=idx,
                    type=detection.label,
                    location="Store",
                    time=humanize.naturaltime(datetime.utcnow() - detection.timestamp),
                    confidence=int(detection.score * 100)
                    if detection.score < 1
                    else int(detection.score),
                    timestamp=detection.timestamp,
                )
            )
        return ObjectDetectionRecentGetResponse(success=True, data=data)

    # noinspection SqlNoDataSourceInspection
    def get_object_detection_summary(
        self, period: Optional[Period] = "today"
    ) -> ObjectDetectionSummaryGetResponse | Error:
        spark = self.spark
        table = self.detection_table_name

        # Step 1: latest_per_object
        latest_per_object = (
            spark.table(table)
            .filter(F.col("label").isNotNull())
            .groupBy("label")
            .agg(F.max("parsed_timestamp").alias("latest_ts"))
        )

        # Step 2: counts_all_time
        counts_all_time = (
            spark.table(table)
            .filter(F.col("label").isNotNull())
            .groupBy("label")
            .agg(F.count("*").alias("total_count"))
        )

        # Step 3: current_window
        detections = spark.table(table).alias("d")
        latest = latest_per_object.alias("l")

        current_window = (
            detections.join(latest, detections.label == latest.label, "inner")
            .filter(
                (detections.parsed_timestamp >= F.date_sub(latest.latest_ts, 6))
                & (detections.parsed_timestamp <= latest.latest_ts)
            )
            .groupBy(detections.label)
            .agg(F.count("*").alias("count_last_7_days"))
        )

        # Step 4: joined
        joined = (
            latest.alias("l")
            .join(counts_all_time.alias("a"), "label", "left")
            .join(current_window.alias("c"), "label", "left")
            .select(
                F.col("l.label").alias("object"),
                F.col("a.total_count"),
                F.col("l.latest_ts").alias("latest_detection"),
                F.coalesce(F.col("c.count_last_7_days"), F.lit(0)).alias(
                    "count_last_7_days"
                ),
                F.round(
                    F.coalesce(F.col("c.count_last_7_days"), F.lit(0))
                    * 100.0
                    / F.nullif(F.col("a.total_count"), F.lit(0)),
                    1,
                ).alias("trend"),
            )
        )

        # Step 5: with_meta
        with_meta = (
            joined.withColumn(
                "icon",
                F.when(F.col("object") == "Vehicles", F.lit("Car"))
                .when(F.col("object") == "People", F.lit("User"))
                .otherwise(F.lit("Box")),
            )
            .withColumn(
                "color",
                F.when(F.col("object") == "Vehicles", F.lit("#3b82f6"))
                .when(F.col("object") == "People", F.lit("#22c55e"))
                .otherwise(F.lit("#a855f7")),
            )
            .orderBy(F.desc("total_count"))
        )

        # Step 6: show or collect
        objects = []
        for row in with_meta.limit(8).collect():
            trend = row["trend"]
            trend = f"+{trend}%" if trend else ""
            objects.append(
                ObjectDetection(
                    object=row.object,
                    count=row.total_count,
                    trend=trend,
                    icon=row.icon,
                    color=row.color,
                )
            )

        return ObjectDetectionSummaryGetResponse(success=True, data=objects)
