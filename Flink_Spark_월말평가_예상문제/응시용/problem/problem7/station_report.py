from pyspark.sql import SparkSession, functions as F

from common import DATA, emit, input_path

def api_summary(stations, readings):
    # TODO TODO 1: DataFrame API
    raise NotImplementedError("TODO 1")

def sql_summary(spark, stations, readings):
    # TODO TODO 2: 임시 뷰와 Spark SQL
    raise NotImplementedError("TODO 2")

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
