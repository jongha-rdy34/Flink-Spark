from pyspark.sql import SparkSession, functions as F

from common import DATA, emit, input_path

def api_summary(stations, readings):
    valid = readings.filter(F.col("units") >= 10)
    return (stations.join(valid, on="station_id", how="left")
            .groupBy("station_id", "zone")
            .agg(F.count("units").alias("valid_count"),
                 F.coalesce(F.sum("units"), F.lit(0)).alias("total_units"))
            .orderBy(F.desc("total_units"), F.asc("station_id")))

def sql_summary(spark, stations, readings):
    stations.createOrReplaceTempView("stations")
    readings.createOrReplaceTempView("readings")
    return spark.sql("""
        SELECT s.station_id, s.zone, COUNT(r.units) AS valid_count,
               COALESCE(SUM(r.units), 0) AS total_units
        FROM stations s
        LEFT JOIN readings r ON s.station_id = r.station_id AND r.units >= 10
        GROUP BY s.station_id, s.zone
        ORDER BY total_units DESC, s.station_id ASC
    """)

def main():
    source = input_path("readings.csv")
    spark = SparkSession.builder.master("local[2]").appName("StationSummary").getOrCreate()
    try:
        stations = spark.read.csv(str(DATA / "stations.csv"), header=True,
                                  schema="station_id STRING, zone STRING")
        readings = spark.read.csv(str(source), header=True,
                                  schema="station_id STRING, units INT")
        emit({"api": [list(row) for row in api_summary(stations, readings).collect()],
              "sql": [list(row) for row in sql_summary(spark, stations, readings).collect()]})
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
