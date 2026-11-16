from pathlib import Path

from pyspark.sql import SparkSession

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# - 목표: RDD 생성, filter, map, flatMap, lazy evaluation을 한 번에 확인합니다.
# - RDD 생성은 sc.parallelize, 결과 확인은 collect를 사용합니다.
# - flatMap은 각 줄을 단어 리스트로 나눈 뒤 하나의 RDD로 펼칩니다.
# - lazy evaluation은 collect 같은 액션을 호출하기 전까지 실제 계산이 지연된다는 점을 확인하세요.

# PySpark RDD 생성 및 변환
# TODO 표시된 부분을 채운 후 PySpark 환경에서 실행하세요.

spark = (
    SparkSession.builder
    .appName("RDDTransformations")
    .getOrCreate()
)
sc = spark.sparkContext


# 1. 1~5까지의 리스트 데이터를 RDD로 변환
numbers_rdd = ____(range(1, 6))
print(numbers_rdd.____())


# 2. 짝수 데이터만 필터링
num_filtered_rdd = numbers_rdd.____(lambda x: x % 2 == 0)
print(num_filtered_rdd.____())


# 3. 모든 숫자를 2배로 변환
doubled_rdd = numbers_rdd.____(lambda x: x * 2)
print(doubled_rdd.____())


# 4. 외부 텍스트 파일 로드
text_rdd = sc.textFile(str(DATA_DIR / "test.txt"))
print(text_rdd.____())


# 5. "Spark"가 포함된 문장 필터링
filtered_rdd = text_rdd.____(lambda line: "Spark" in line)
print(filtered_rdd.____())


# 6. 문장을 단어 단위로 분해
words_rdd = text_rdd.____(lambda line: line.split(" "))
print(words_rdd.____())


# 7. Lazy Evaluation 실습
lazy_rdd = (
    numbers_rdd
    .____(lambda x: x * 2)
    .____(lambda x: x > 5)
)


# 8. 생성 완료 후 잠시 대기
print("lazy_rdd 정의 완료. 이 변환 체인은 아직 계산하지 않았습니다.")
input("Spark UI 확인 후 Enter를 눌러 collect 실행: ")


# 9. 실제 연산 실행 (collect 호출)
print(lazy_rdd.____())

# Spark 세션 종료
spark.stop()
