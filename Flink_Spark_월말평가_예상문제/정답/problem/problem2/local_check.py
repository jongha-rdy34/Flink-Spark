from pyspark.sql import SparkSession

from common import emit

def make_session():
    return (SparkSession.builder.master("local[2]")
            .appName("StationDispatch")
            .getOrCreate())

def summarize(sc, values):
    records = sc.parallelize(values, 2)
    return {"master": sc.master, "partitions": records.getNumPartitions(),
            "count": records.count()}

def main():
    spark = make_session()
    try:
        emit(summarize(spark.sparkContext, [14, 4, 14, 0, 20]))
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
