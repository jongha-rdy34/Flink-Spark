"""완료된 주문의 카테고리별 매출을 Spark SQL로 집계한다.
실행: python sales_sql.py
"""
from pathlib import Path

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SalesSQL").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

DATA_PATH = Path(__file__).resolve().parent / "data" / "sales.csv"

# 제공: 헤더가 있는 CSV를 타입 추론으로 읽는다.
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(str(DATA_PATH))
)
df.printSchema()

# TODO 1: 아래 raise를 df를 sales 임시 뷰로 등록하는 코드로 교체하시오.
⟪raise NotImplementedError("임시 뷰 등록 코드 작성")‖df.createOrReplaceTempView("sales")⟫

# TODO 2: 아래 query 문자열을 문제의 조건에 맞는 SELECT 문으로 작성하시오.
query = """
⟪    -- TODO 2: SELECT 문 작성
‖    SELECT category,
           COUNT(*) AS order_count,
           SUM(price * qty) AS revenue
    FROM sales
    WHERE status = 'DONE'
    GROUP BY category
    ORDER BY revenue DESC, category ASC
⟫"""

result = spark.sql(query)
result.show(truncate=False)
print(f"ROWS={result.count()}")

spark.stop()
