# 빈칸(____)을 채워 아래 데이터 처리 단계를 구현하세요.
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("RDDPartitionStructure")
    .getOrCreate()
)
sc = spark.sparkContext

# 작은 입력으로 재분배 전후 파티션별 데이터를 관찰합니다.
numbers = sc.parallelize(range(1, 101), 4)
n = numbers.____()
expanded = numbers.____(2 * n)
reduced = expanded.____(n)

print("원본 파티션 수:", numbers.getNumPartitions())
print("원본 파티션별 데이터:", numbers.glom().collect())
print("재분배 후 파티션 수:", expanded.getNumPartitions())
print("재분배 후 파티션별 데이터:", expanded.glom().collect())
print("병합 후 파티션 수:", reduced.getNumPartitions())
print("병합 후 파티션별 데이터:", reduced.glom().collect())
print("repartition은 데이터 정렬 순서를 보장하지 않습니다.")
spark.stop()
