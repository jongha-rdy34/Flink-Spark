from pathlib import Path

from pyspark.sql import SparkSession

# 실습 파일 원본은
# ws4-3 -- data -- text.txt
#       |
#       -- skeleton -- skeleton.py

# 그래서 기존 DIR에서 parent하나 뺌
# DATA_DIR = Path(__file__).resolve().parent.parent / "data"

DATA_DIR = Path(__file__).resolve().parent / "data"

# - 목표: sc.textFile로 외부 파일을 읽고 collect/count/map으로 내용을 탐색합니다.
# - print TODO에는 RDD 액션 결과가 들어가야 합니다. 예: collect(), count().
# - 대소문자 변환은 문자열 메서드 upper(), lower(), 길이는 len(line)을 사용합니다.


# SparkSession 생성
spark = (
    ________.________
    .appName("LoadExternalFileApp")
    .________()
)
sc = spark.________


# 1. 텍스트 파일 로드
text_data = sc.textFile(str(DATA_DIR / "test.txt"))


# 2. 전체 데이터 확인
print(____)


# 3. 전체 줄 개수 확인
print(____)


# 4. 모든 문장을 대문자로 변환 후 출력
upper_case_data = text_data.____(lambda line: ____)
print(upper_case_data.collect())


# 5. 모든 문장을 소문자로 변환 후 출력
lower_case_data = text_data.____(lambda line: ____)
print(lower_case_data.collect())


# 6. 각 문장의 길이 출력
line_lengths = text_data.____(lambda line: ____)
print(line_lengths.collect())

# Spark 세션 종료
spark.stop()
