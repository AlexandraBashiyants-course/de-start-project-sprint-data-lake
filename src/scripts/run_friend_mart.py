# run_friend_mart.py
from friend_recommendation_mart_builder import FriendRecommendationMart
from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("FriendRecommendationMart") \
        .config("spark.driver.memoryOverhead", "5g") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    try:
        builder = FriendRecommendationMart(
            spark=spark,
            events_path="/user/master/data/geo/events/date=2022-05-01", #для уменьшения объема
            user_mart_path="/user/kasssssand/data/sprint_3/marts/user_geo_mart"
        )
        df = builder.build()
        
        print(f"Result count: {df.count()}")
        df.show(5)

        df.write.mode("overwrite").parquet("/user/kasssssand/data/sprint_3/marts/friend_geo_mart")

    except Exception as e:
        logger.error(f"Job failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()