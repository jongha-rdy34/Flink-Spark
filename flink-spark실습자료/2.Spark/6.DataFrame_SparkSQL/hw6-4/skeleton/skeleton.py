"""30세 이상 고객의 국가별 집계를 DataFrame API와 Spark SQL로 구현한다."""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
)


# 1. Spark 실행 환경과 파일 경로
spark = (
    SparkSession.builder
    .appName("CustomerSummaryApiAndSql")
    .getOrCreate()
)
data_path = Path(__file__).resolve().parent.parent / "data" / "people.csv"


# 2. 고객 CSV 읽기와 스키마 확인
people_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(str(data_path))
)
people_df.printSchema()


# 3. DataFrame API로 국가별 집계
# TODO: Age >= 30 필터, 국가별 그룹화, 인원 수와 평균 나이 집계
api_result = (
    people_df
    .____(col("Age") >= 30)
    .____("Geography")
    .agg(
        ____("*").alias("customer_count"),
        ____("Age").alias("average_age"),
    )
    .orderBy(col("customer_count").desc(), col("Geography").asc())
)


# 4. SQL 임시 뷰 등록과 동일 집계
# TODO: 임시 뷰를 등록하고 같은 조건·집계·정렬을 SQL로 작성
people_df.____("people")
sql_result = spark.sql(
    """
    SELECT Geography,
           COUNT(*) AS customer_count,
           AVG(Age) AS average_age
    FROM people
    WHERE ____
    GROUP BY ____
    ORDER BY customer_count DESC, Geography ASC
    """
)


# 5. 두 집계 결과 출력
print("=== DataFrame API 결과 ===")
api_result.show(truncate=False)
print("=== Spark SQL 결과 ===")
sql_result.show(truncate=False)


# 6. Spark 세션 종료
spark.stop()
