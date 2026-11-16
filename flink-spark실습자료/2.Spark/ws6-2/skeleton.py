from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# DataFrame 스키마(schema) 정의 및 데이터 타입 변환
# 아래의 빈칸(____)을 채운 후 PySpark 환경에서 실행하세요.
spark = (
    SparkSession.builder
    .appName("StudentsData")
    .getOrCreate()
)


# 1. 스키마 정의
schema = StructType(
    [
        StructField("Name", StringType(), True),
        StructField("Major", StringType(), True),
        StructField("Age", StringType(), True),
    ]
)


# 2. CSV 파일 로드 (header 없음)
df = spark.read.csv(str(DATA_DIR / "students.csv"), schema=schema)


# 3. 전체 데이터 출력
df.____


# 4. Age 컬럼을 정수형으로 변환
df_casted = df.withColumn("____", col("____").cast(IntegerType()))

# 스키마 출력
df_casted.____


# 5. Age ≥ 25 학생 필터링 (이름, 전공 출력)
(
    df_casted
    .____(col("Age") >= 25)
    .____("Name", "Major")
    .show()
)


# 6. Major 기준 그룹화 후 전공별 학생 수 출력
(
    df_casted
    .____("Major")
    .____()
    .show()
)

# Spark 세션 종료
spark.stop()
