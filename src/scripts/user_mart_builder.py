# /lessons/scripts/user_mart_builder.py

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import broadcast
import logging
from typing import Tuple

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class UserGeoMart:
    def __init__(self, spark: SparkSession, events_path: str, cities_path: str):
        self.spark = spark
        self.events_path = events_path
        self.cities_path = cities_path

    def load_data(self) -> Tuple[DataFrame, DataFrame]:
        logger.info(f"Loading events from {self.events_path}")

        # 🔽 Читаем ТОЛЬКО одну дату — путь должен быть точным
        raw_events = self.spark.read.parquet("/user/master/data/geo/events/date=2022-05-01")
#         raw_events = raw_events.filter(F.to_date(F.col("event.message_ts")) == "2022-05-01")
#         raw_events = self.spark.read.parquet("/user/master/data/geo/events/date=2022-05-01")
        raw_events.printSchema()
        # Фильтруем только сообщения
        
        messages = raw_events.filter(F.col("event_type") == "message") \
        .select(
            F.col("event.message_from").alias("user_id"),
            "lat", "lon",
            F.col("event.message_id").alias("msg_id"),
            F.col("event.datetime").alias("datetime")
        ) \
            .filter(
                F.col("user_id").isNotNull() &
                F.col("lat").isNotNull() &
                F.col("lon").isNotNull()
            ).sample(0.0001, seed=42)

        # 🔽 УМЕНЬШАЕМ ДАННЫЕ МАКСИМАЛЬНО
#         messages = messages

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
                .filter(F.col("city").isin("Sydney")) # также уменьшили из-за памяти

            # Карта временных зон
            timezone_map = {"Sydney": "Australia/Sydney"}
            tz_expr = F.create_map([F.lit(x) for pair in timezone_map.items() for x in pair])
            cities = cities.withColumn("city_timezone", tz_expr[F.col("city")])

        except Exception as e:
            logger.error(f"Failed to read or parse cities file: {e}")
            raise

        return messages, cities

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

    def build(self) -> DataFrame:
        messages, cities = self.load_data()

        # Найти ближайший город для каждого сообщения
        user_with_city = messages.crossJoin(broadcast(cities)) \
            .withColumn("distance", self.haversine_distance(
                F.col("lat"), F.col("lon"),
                F.col("city_lat"), F.col("city_lng")
            )) \
            .withColumn("rn", F.row_number().over(
                Window.partitionBy("user_id", "msg_id").orderBy(F.col("distance").asc())
            )) \
            .filter(F.col("rn") == 1) \
            .select("user_id", "city", "city_timezone", "datetime")

        # Актуальный город — последний по времени
        window_last = Window.partitionBy("user_id").orderBy(F.desc("datetime"))
        latest_city = user_with_city.withColumn("rn", F.row_number().over(window_last)) \
            .filter(F.col("rn") == 1) \
            .select("user_id", F.col("city").alias("act_city"), "city_timezone", "datetime")

        # Домашний город — упрощённо: первый город (или Unknown)
        home_cities = user_with_city.groupBy("user_id") \
            .agg(F.first("city").alias("home_city"))

        # Путешествия
        travel = user_with_city.groupBy("user_id") \
            .agg(
                F.count("city").alias("travel_count"),
                F.collect_list("city").alias("travel_array")
            )

        # Местное время
        result = latest_city.alias("l") \
            .join(home_cities.alias("h"), on="user_id", how="left") \
            .join(travel.alias("t"), on="user_id", how="left") \
            .withColumn("local_time", F.from_utc_timestamp(F.col("datetime"), F.col("city_timezone"))) \
            .select(
                "user_id",
                "act_city",
                F.coalesce(F.col("home_city"), F.lit("Unknown")).alias("home_city"),
                F.coalesce(F.col("travel_count"), F.lit(0)).alias("travel_count"),
                F.coalesce(F.col("travel_array"), F.array()).alias("travel_array"),
                "local_time"
            )

        return result