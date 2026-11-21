"""제공 코드: 수정하지 않는다.
Transformation 정의와 Action 실행 시점을 Spark UI의 Jobs 화면으로 확인한다.
실행: python lazy_check.py
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("LazyCheck").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext

numbers = sc.parallelize(range(1, 11), 2)
lazy_rdd = numbers.map(lambda x: x * 3).filter(lambda x: x % 2 == 0)

print("STEP1 transformation defined", flush=True)
print("Spark UI:", sc.uiWebUrl, flush=True)
input("[1] Spark UI의 Jobs 화면을 캡처한 뒤 Enter: ")

print("RESULT", lazy_rdd.collect(), flush=True)
print("COUNT", lazy_rdd.count(), flush=True)
input("[2] Spark UI의 Jobs 화면을 다시 캡처한 뒤 Enter: ")

spark.stop()
