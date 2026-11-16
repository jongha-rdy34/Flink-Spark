import argparse
from pathlib import Path

from pyspark.sql import SparkSession

from common import emit

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--minimum", type=int, required=True)
    args = parser.parse_args()
    spark = SparkSession.builder.master("local[2]").appName("ReadingInspection").getOrCreate()
    try:
        values = spark.sparkContext.textFile(str(args.input)).map(int)
        selected = values.filter(lambda value: value >= args.minimum)
        emit({"minimum": args.minimum, "count": selected.count(),
              "values": sorted(selected.collect())})
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
