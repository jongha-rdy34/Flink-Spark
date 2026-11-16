# DataFrame 생성 및 기본 연산
# 아래의 빈칸(____)을 채운 후 PySpark 환경에서 실행하세요.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

spark = (
    SparkSession.builder
    .appName("DataFrameBasic")
    .getOrCreate()
)


# 1. 데이터 생성
data = [
    ("Alice", "Data Science", 25),
    ("Bob", "Machine Learning", 30),
    ("Charlie", "Data Engineering", 35),
    ("David", "Data Science", 40),
    ("Eva", "Machine Learning", 28),
]
columns = ["Name", "Department", "Age"]

df = spark.createDataFrame(data, columns)


# 2. 전체 데이터 출력
df.____()


# 3. 'Department'가 "Data Science"인 데이터 필터링
(
    df
    .____(col("Department") == "Data Science")
    .show()
)


# 4. 'Name', 'Age' 컬럼만 선택
(
    df
    .____("Name", "Age")
    .show()
)


# 5. 'Age' 기준 내림차순 정렬
(
    df
    .____(col("Age").desc())
    .show()
)


# 6. 'Department'별 평균 나이 계산
(
    df
    .____("Department")
    .____(avg("Age"))
    .show()
)


# 7. 스키마 확인
df.printSchema()

# Spark 세션 종료
spark.stop()
