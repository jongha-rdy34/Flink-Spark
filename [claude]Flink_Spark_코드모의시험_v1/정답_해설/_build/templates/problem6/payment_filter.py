"""결제 완료 건만 골라 결제 수단별로 집계한다.
실행: python payment_filter.py
"""
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

spark = SparkSession.builder.appName("PaymentFilter").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

DATA_PATH = Path(__file__).resolve().parent / "data" / "payments.csv"

# 제공: 헤더 없는 CSV를 모든 컬럼 StringType 스키마로 읽는다.
schema = StructType([
    StructField("pay_id", StringType(), True),
    StructField("user", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("status", StringType(), True),
    StructField("method", StringType(), True),
])
df = spark.read.csv(str(DATA_PATH), header=False, schema=schema)

# TODO 1: 아래 raise를 amount 컬럼을 IntegerType으로 변환한 DataFrame을 df_casted에 저장하는 코드로 교체하시오.
# 컬럼 이름은 amount 그대로 유지한다.
⟪raise NotImplementedError("타입 변환 코드 작성")‖df_casted = df.withColumn("amount", col("amount").cast(IntegerType()))⟫
df_casted.printSchema()

# TODO 2: 아래 filter 조건을 status 값이 정확히 "PAID"인 행만 선택하도록 수정하시오.
# "UNPAID", "REPAID"처럼 PAID를 포함하는 다른 상태는 제외하며, 아래 집계·출력 코드는 바꾸지 않는다.
paid = df_casted.filter(⟪col("status").contains("PAID")‖col("status") == "PAID"⟫)

summary = (
    paid
    .groupBy("method")
    .agg(count("*").alias("pay_count"), sum("amount").alias("total_amount"))
    .orderBy("method")
)
summary.show(truncate=False)
print(f"COUNT={paid.count()}")
print(f"TOTAL={paid.agg(sum('amount')).collect()[0][0]}")

spark.stop()
