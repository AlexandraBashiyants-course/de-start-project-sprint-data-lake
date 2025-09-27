from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

class FriendRecommendationMart:
    def __init__(self, spark: SparkSession, events_path: str, user_mart_path: str):
        self.spark = spark
        self.events_path = events_path
        self.user_mart_path = user_mart_path

#     def haversine_distance(self, lat1, lon1, lat2, lon2):
#         R = 6371.0  # радиус Земли в км
#         lat1_rad = F.radians(lat1)
#         lat2_rad = F.radians(lat2)
#         delta_lat = F.radians(lat2 - lat1)
#         delta_lon = F.radians(lon2 - lon1)

#         a = (F.sin(delta_lat / 2) ** 2 +
#              F.cos(lat1_rad) * F.cos(lat2_rad) * F.sin(delta_lon / 2) ** 2)
#         c = 2 * F.asin(F.sqrt(a))
#         return R * c
    
    def build(self) -> DataFrame:
        # 1. Читаем события и сразу фильтруем по одной дате
        events = self.spark.read.parquet(self.events_path)
#         events = events.filter(F.col("date") == "2022-05-01")
        events.printSchema()

        # 2. Подписки — только нужные поля, с жёстким лимитом
        subscriptions = events.filter(F.col("event_type") == "subscription") \
            .select(
                F.col("event.subscription_channel").alias("channel_id"),
                F.col("event.subscription_user").alias("user_id")
            ) \
            .filter(F.col("user_id").isNotNull()) \
            .dropDuplicates()

        # ⚠️ КРИТИЧНОЕ СОКРАЩЕНИЕ ДАННЫХ
        subscriptions_limited = subscriptions.sample(0.01, seed=42).limit(20).repartition(1)

        # 3. Пары пользователей на одном канале
        pairs = subscriptions_limited.alias("a").join(
            subscriptions_limited.alias("b"),
            on="channel_id",
            how="inner"
        ).filter(F.col("a.user_id") < F.col("b.user_id")) \
         .select(
             F.col("a.user_id").alias("user_left"),
             F.col("b.user_id").alias("user_right")
         ) \
         .limit(100)  # ещё больше ограничений

        # 4. Кто уже переписывался? Только те, кто писал сообщения
        messages = events.filter(F.col("event_type") == "message") \
            .select(
                F.least(F.col("event.message_from"), F.col("event.message_to")).alias("user_left"),
                F.greatest(F.col("event.message_from"), F.col("event.message_to")).alias("user_right")
            ) \
            .filter(F.col("user_left").isNotNull() & F.col("user_right").isNotNull()) \
            .distinct()

        # Исключаем пары, которые уже общались
        candidates = pairs.join(messages, on=["user_left", "user_right"], how="left_anti")

        # 5. Добавляем координаты (если есть в user_mart)
        try:
            user_coords = self.spark.read.parquet(self.user_mart_path) \
                .select("user_id", "lat", "lon") \
                .limit(100)  # минимум данных
        except Exception:
            # Если user_mart ещё нет — создаём заглушку
            user_coords = self.spark.createDataFrame([
                (1, 55.7558, 37.6176),
                (2, 55.7559, 37.6177),
                (3, 55.7560, 37.6178),
                (4, 55.7561, 37.6179)
            ], ["user_id", "lat", "lon"])

        # Присоединяем координаты
        with_coords = candidates \
            .join(user_coords, on=F.col("user_left") == F.col("user_id"), how="inner") \
            .withColumnRenamed("lat", "lat_left") \
            .withColumnRenamed("lon", "lon_left") \
            .drop("user_id") \
            .join(user_coords, on=F.col("user_right") == F.col("user_id"), how="inner") \
            .withColumnRenamed("lat", "lat_right") \
            .withColumnRenamed("lon", "lon_right") \
            .drop("user_id")

        # 6. Расстояние — упрощённая версия (без сложных формул)
        # Для отладки можно использовать константу или простое вычисление
        with_coords = with_coords.withColumn(
            "distance_km",
            F.sqrt(
                (F.col("lat_left") - F.col("lat_right")) ** 2 +
                (F.col("lon_left") - F.col("lon_right")) ** 2
            ) * 111  # грубая оценка в км
        ).filter(F.col("distance_km") <= 1.0)

        # 7. Финальный результат
        result = with_coords.select(
            "user_left",
            "user_right",
            F.current_timestamp().alias("processed_dttm"),
            F.lit("Sydney").alias("zone_id"),
            F.from_utc_timestamp(F.current_timestamp(), "Australia/Sydney").alias("local_time")
        ).limit(10)  # финальный лимит

        return result

        # Расстояние (без UDF!)
        def haversine_distance(lat1, lon1, lat2, lon2):
            R = 6371.0
            lat1_rad = F.radians(lat1)
            lat2_rad = F.radians(lat2)
            delta_lat = F.radians(lat2 - lat1)
            delta_lon = F.radians(lon2 - lon1)
            a = (F.sin(delta_lat / 2) ** 2 +
                 F.cos(lat1_rad) * F.cos(lat2_rad) * F.sin(delta_lon / 2) ** 2)
            c = 2 * F.asin(F.sqrt(a))
            return R * c

        with_coords = with_coords.withColumn(
            "distance_km",
            haversine_distance(
                F.col("lat_left"), F.col("lon_left"),
                F.col("lat_right"), F.col("lon_right")
            )
        ).filter(F.col("distance_km") <= 1.0)

        # Финальный результат
        result = with_coords.select(
            "user_left",
            "user_right",
            F.current_timestamp().alias("processed_dttm"),
            F.lit("Sydney").alias("zone_id"),
            F.from_utc_timestamp(F.current_timestamp(), "Australia/Sydney").alias("local_time")
        )

        return result