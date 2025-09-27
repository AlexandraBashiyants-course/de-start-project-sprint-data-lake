from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
import logging
from pyspark.sql.functions import broadcast
# Настройка логгера
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ZoneGeoMart:
    def __init__(self, spark: SparkSession, events_path: str, cities_path: str):
        self.spark = spark
        self.events_path = events_path
        self.cities_path = cities_path
        
        
    def haversine_distance(self, lat1, lon1, lat2, lon2):
        """
        Вычисляет расстояние между двумя точками на сфере по формуле гаверсинуса.
        """
        R = 6371.0  # радиус Земли в км
        lat1_rad = F.radians(lat1)
        lat2_rad = F.radians(lat2)
        delta_lat = F.radians(lat2 - lat1)
        delta_lon = F.radians(lon2 - lon1)

        a = (F.sin(delta_lat / 2) ** 2 +
             F.cos(lat1_rad) * F.cos(lat2_rad) * F.sin(delta_lon / 2) ** 2)
        c = 2 * F.asin(F.sqrt(a))
        return R * c
    
    
    def load_and_enrich(self) -> DataFrame:
        logger.info(f"Loading events from {self.events_path}")

        # 🔽 Фильтр: только одна дата
        raw_events = self.spark.read.parquet(self.events_path)
        raw_events = raw_events.filter(F.col("date") == "2022-05-01")

        # Фильтруем только сообщения
        messages = raw_events.filter(F.col("event_type") == "message")

        # Извлекаем минимальный набор полей
        enriched_messages = messages.select(
            F.col("event.message_from").alias("user_id"),
            "lat",
            "lon",
            F.col("event.message_id").alias("msg_id"),
            "date",
            "event_type"  # ← обязательно!
        ).filter(
            F.col("user_id").isNotNull() &
            F.col("lat").isNotNull() &
            F.col("lon").isNotNull()
        )

        # 🔽 УМЕНЬШАЕМ ДАННЫЕ ДЛЯ ОТЛАДКИ
        enriched_messages = enriched_messages \
            .sample(0.001, seed=42) \
            .limit(50) \
            .repartition(1)

        logger.info(f"Loading cities from {self.cities_path}")
        try:
            cities_raw = self.spark.read.csv(
                self.cities_path,
                header=True,
                schema="id STRING, city STRING, lat STRING, lng STRING"
            )

            # Преобразуем координаты
            cities = cities_raw \
                .withColumn("lat", F.regexp_replace(F.col("lat"), ",", ".").cast("double")) \
                .withColumn("lng", F.regexp_replace(F.col("lng"), ",", ".").cast("double")) \
                .withColumnRenamed("lat", "city_lat") \
                .withColumnRenamed("lng", "city_lng") \
                .filter(F.col("city").isin("Sydney"))

            # Карта временных зон
            timezone_map = {"Sydney": "Australia/Sydney"}
            tz_expr = F.create_map([F.lit(x) for pair in timezone_map.items() for x in pair])
            cities = cities.withColumn("city_timezone", tz_expr[F.col("city")])

        except Exception as e:
            logger.error(f"Failed to read or parse cities file: {e}")
            raise

        # Найти ближайший город для каждого сообщения
        user_with_city = enriched_messages.crossJoin(broadcast(cities)) \
            .withColumn("distance", self.haversine_distance(
                F.col("lat"), F.col("lon"),
                F.col("city_lat"), F.col("city_lng")
            )) \
            .withColumn("rn", F.row_number().over(
                Window.partitionBy("user_id", "msg_id").orderBy(F.col("distance"))
            )) \
            .filter(F.col("rn") == 1) \
            .select("user_id", "lat", "lon", "msg_id", "date", "event_type", "city", "city_timezone")

        return user_with_city


    def build(self) -> DataFrame:
        df = self.load_and_enrich()

        # Преобразуем date в тип DateType
        df = df.withColumn("date", F.to_date(F.col("date")))

        # Создаём колонку week — начало недели
        df = df.withColumn("week", F.date_trunc("week", F.col("date")))

        # Находим последнюю зону для каждого пользователя (для справки, не используется)
        window_last = Window.partitionBy("user_id").orderBy(F.desc("date"))
        latest_city = df.withColumn("rn", F.row_number().over(window_last)) \
            .filter(F.col("rn") == 1) \
            .select(
                "user_id",
                F.col("city").alias("zone_id"),
                "city_timezone"
            )

        # Агрегируем события по неделям и городам
        weekly = df.groupBy("week", "city").agg(
            F.sum(F.when(F.col("event_type") == "message", 1).otherwise(0)).alias("week_message"),
            F.sum(F.when(F.col("event_type") == "reaction", 1).otherwise(0)).alias("week_reaction"),
            F.sum(F.when(F.col("event_type") == "subscription", 1).otherwise(0)).alias("week_subscription"),
            F.sum(F.when(F.col("event_type") == "registration", 1).otherwise(0)).alias("week_user")
        )

        # Формируем финальный результат БЕЗ джойна
        result = weekly.select(
            "week",
            F.col("city").alias("zone_id"),
            "week_message",
            "week_reaction",
            "week_subscription",
            "week_user"
        )

        return result