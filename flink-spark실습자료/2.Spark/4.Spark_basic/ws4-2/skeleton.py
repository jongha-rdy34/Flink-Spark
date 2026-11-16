# - 목표: 직접 만든 RDD에 map/filter/count/collect를 적용해 기본 변환을 익힙니다.
# - SparkSession 생성, sc.parallelize, len, map, filter, collect를 순서대로 채우세요.
# - filter는 조건을 만족하는 원소만 남기고, count는 원소 개수만 반환합니다.
# - "is" 필터는 부분 문자열이 아니라 독립 단어인지 확인하도록 split()을 사용합니다.

from pyspark.sql import SparkSession

# SparkSession 생성
spark = (
    ________.________
    .appName("WordLengthApp")
    .________()
)
sc = spark.________


# 1. 단어 길이 변환 및 필터링
# 문자열 데이터 생성("Spark", "Parallel", "Machine", "Learning", "Hadoop", "Kafka", "Big Data")
words = ____(
    [
        "Spark",
        "Parallel",
        "Machine",
        "Learning",
        "Hadoop",
        "Kafka",
        "Big Data",
    ]
)

# 생성한 문자열 데이터 확인
print(words.collect())

# 각 단어의 길이 계산
word_lengths = words.____(lambda word: ____(word))
print(word_lengths.collect())

# 6글자 이상 단어만 필터링
long_words = words.filter(lambda word: ____(word) >= 6)
print(long_words.collect())

# 6글자 이상 단어 개수
long_words_count = long_words.____()
print(f"6글자 이상 단어 개수: {long_words_count}")


# 2. 짝수와 홀수 필터링
# 1~20까지 숫자 데이터를 생성
numbers = ____(range(1, 21))

# 짝수 필터링 후 데이터 확인
even = numbers.filter(lambda x: x % 2 == 0)
print("짝수:", even.collect())

# 홀수 필터링 후 데이터 확인
odd = numbers.filter(lambda x: x % 2 != 0)
print("홀수:", odd.collect())


# 3. 소문자 변환 및 "is" 포함 문장 필터링
# 문자열 데이터 생성
sentences = ____(
    [
        "Spark is a powerful analytics engine",
        "Big Data is transforming industries",
        "Data Science is revolutionizing decision making",
        "Machine Learning and AI are the future",
    ]
)

# 모든 문장을 소문자로 변환하여 데이터 확인
lower = sentences.map(lambda line: line.____())
print("소문자 변환:", lower.____())

# "is"라는 독립 단어가 포함된 문장만 필터링 후 데이터 확인
contains_is = lower.____(
    lambda line: "is" in line.lower().____()
)  # TODO: filter 연산과 단어 단위 분리를 위한 문자열 메서드
print('"is" 포함 문장:', contains_is.____())

# Spark 세션 종료
spark.stop()
