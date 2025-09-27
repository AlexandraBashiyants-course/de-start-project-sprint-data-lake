# /lessons/scripts/run_zone_mart.py

from zone_mart_builder import ZoneGeoMart
from pyspark.sql import SparkSession
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ZoneGeoMart") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    events_path = "/user/master/data/geo/events"
    cities_path = "/user/kasssssand/geo.csv"
    output_path = "/user/kasssssand/data/sprint_3/marts/zone_geo_mart"

    try:
        builder = ZoneGeoMart(spark, events_path, cities_path)
        result = builder.build()
        
        print("=== Columns ===")
        print(result.columns)

        result.write.mode("overwrite").parquet(output_path)
        logger.info("Zone mart successfully written.")

    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
    finally:
        spark.stop()