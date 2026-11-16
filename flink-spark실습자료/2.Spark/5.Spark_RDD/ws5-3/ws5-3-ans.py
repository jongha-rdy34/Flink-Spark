# 빈칸에 RDD 생성·결합·중복 제거·키 생성·키별 집계 API를 채우세요.
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("RDDUnionDistinctKeyCount")
    .getOrCreate()
)
sc = spark.sparkContext


# 1. 중복 단어가 포함된 두 목록을 각각 RDD로 만듭니다.
words_a = ["Spark", "Kafka", "Spark", "Python"]
words_b = ["Kafka", "SQL", "Spark"]

rdd_a = sc.parallelize(words_a)
rdd_b = sc.parallelize(words_b)
print("RDD A:", rdd_a.collect())
print("RDD B:", rdd_b.collect())


# 2. 중복을 유지한 채 두 RDD를 결합하고 원소 수를 확인합니다.
merged_rdd = rdd_a.union(rdd_b)
print("결합 결과:", merged_rdd.collect())
print("결합 후 원소 수:", merged_rdd.count())


# 3. 같은 값을 하나만 남기고 원소 수를 비교합니다.
unique_rdd = merged_rdd.distinct()
print("중복 제거 결과:", unique_rdd.collect())
print("중복 제거 후 원소 수:", unique_rdd.count())


# 4. 중복 제거 전의 RDD에서 각 단어의 첫 글자를 키로 붙입니다.
keyed_rdd = merged_rdd.keyBy(lambda word: word[0])
print("첫 글자를 키로 지정한 결과:", keyed_rdd.collect())


# 5. 첫 글자별 원소 개수를 집계합니다.
key_counts = keyed_rdd.countByKey()
print("첫 글자별 개수:", dict(key_counts))

spark.stop()
