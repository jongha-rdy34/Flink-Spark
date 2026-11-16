from pyspark.sql import SparkSession


# 1. Spark 실행 환경
spark = (
    SparkSession.builder
    .appName("PartitionApp")
    .getOrCreate()
)
sc = spark.sparkContext


# 2. 원본 RDD 생성
numbers = sc.parallelize(range(1, 11), 4)


# 3. 단일 파티션으로 재분배
single = numbers.repartition(1)
print("기본 파티션 개수:", numbers.getNumPartitions())
print("변경된 파티션 개수:", single.getNumPartitions())
print("원본 파티션별 데이터:", numbers.glom().collect())
print("단일 파티션 데이터:", single.glom().collect())
print("repartition은 데이터의 정렬 순서를 보장하지 않습니다.")

spark.stop()
