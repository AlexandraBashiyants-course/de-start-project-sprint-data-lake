# /lessons/scripts/run_user_mart.py

from user_mart_builder import UserGeoMart
from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("UserGeoMart") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    events_path = "/user/master/data/geo/events"
    cities_path = "/user/kasssssand/geo.csv"
    output_path = "/user/kasssssand/data/sprint_3/marts/user_geo_mart"

    try:
        builder = UserGeoMart(spark, events_path, cities_path)
        result = builder.build()
        
        print("=== Columns ===")
        print(result.columns)

        result.write.mode("overwrite").parquet(output_path)
        logger.info("User mart successfully written.")

    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
    finally:
        spark.stop()