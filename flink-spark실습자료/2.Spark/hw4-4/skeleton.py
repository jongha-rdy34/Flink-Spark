from pathlib import Path

from pyspark.sql import SparkSession

DATA_DIR = Path(__file__).resolve().parent.parent / "data"


# 1. Spark 실행 환경
# - 목표: 외부 텍스트 파일을 RDD로 읽고 collect/count/filter/map/repartition을 연습합니다.
# - SparkSession 생성 후 sc = spark.sparkContext를 완성하세요.
# - contains_data는 line.lower()에 "data"가 포함되는지 확인하면 됩니다.


# SparkSession 생성
spark = (
    ________.________
    .appName("TextDataAnalysis")
    .________()
)
sc = spark.________


# 2. 텍스트 파일 읽기
text_data = sc.textFile(str(DATA_DIR / "test_1.txt"))

# 전체 텍스트 출력
print(text_data.collect())

# 줄 수 출력
print("전체 줄 개수:", text_data.count())


# 3. 키워드가 포함된 문장 선택
contains_data = text_data.filter(lambda line: ____ in line.____())
print(contains_data.collect())

# 포함된 줄 수 출력
print("'data'가 포함된 줄 개수:", contains_data.count())


# 4. 대문자·소문자 변환
upper_case_data = text_data.map(lambda line: line.____())
print(upper_case_data.collect())

# 소문자로 변환
lower_case_data = text_data.map(lambda line: line.____())
print(lower_case_data.collect())


# 5. 파티션 수 확인과 재분배
print("기본 파티션 개수:", text_data.____())

# 파티션 4개로 재설정 후 확인
repartitioned_data = text_data.____(____)
print("변경된 파티션 개수:", repartitioned_data.____())

# Spark 세션 종료
spark.stop()
