from pathlib import Path

from pyspark.sql import SparkSession

DATA_DIR = Path(__file__).resolve().parent.parent / "data"

# - 목표: 텍스트를 단어로 분해하고 stopwords 제거 후 단어별 개수를 계산합니다.
# - 줄 단위 RDD를 단어 단위로 펼칠 때는 flatMap을 사용합니다.
# - 단어 개수 계산은 (word, 1) 쌍을 만든 뒤 reduceByKey로 합산합니다.

# WordCount
# TODO 표시된 부분을 채운 후 PySpark 환경에서 실행하세요.

spark = (
    SparkSession.builder
    .appName("WordCountApp")
    .getOrCreate()
)
sc = spark.sparkContext


# 1. 텍스트 파일 RDD 변환
word_count_rdd = sc.textFile(str(DATA_DIR / "wordCount.txt"))


# 2. 불필요한 단어 목록 정의 (Stopwords)
stopwords = {
    "and",
    "is",
    "the",
    "on",
    "of",
    "to",
    "a",
    "has",
    "for",
    "from",
    "that",
    "this",
    "it",
    "an",
    "be",
    "are",
    "by",
    "in",
    "with",
    "as",
}


# 3. 줄을 단어 단위로 분리
words = word_count_rdd.____(lambda line: line.split())
print(words.____())


# 4. 모든 단어를 소문자로 변환
lower_words = words.____(lambda word: word.lower())
print(lower_words.____())


# 5. 최소 길이 2글자 이상만 필터링
filtered_length = lower_words.____(lambda word: len(word) >= 2)
print(filtered_length.____())


# 6. Stopwords 제거
filtered_words = filtered_length.____(lambda word: word not in ____)
print(filtered_words.____())


# 7. (단어, 1) 형태로 매핑
word_pairs = filtered_words.____(lambda word: (word, ____))
print(word_pairs.____())


# 8. 단어별 빈도 계산
word_counts = word_pairs.____(lambda a, b: a + b)
print(word_counts.____())  # 전체 결과 확인: 출력 순서는 평가하지 않음

# Spark 세션 종료
spark.stop()
