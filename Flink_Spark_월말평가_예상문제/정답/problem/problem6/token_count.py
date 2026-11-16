from pyspark.sql import SparkSession

from common import emit, input_path

STOP = {"a", "the"}

def normalize(lines):
    # TODO 1: 공백 단위로 펼치고 소문자로 변환한 토큰 RDD 반환
    return lines.flatMap(lambda line: line.split()).map(lambda token: token.lower())

def top_tokens(lines, limit):
    tokens = normalize(lines)
    # 오류 수정 대상: 부분 문자열까지 제외되는 조건을 고치세요.
    kept = tokens.filter(lambda token: token not in STOP)
    counts = kept.map(lambda token: (token, 1)).reduceByKey(lambda a, b: a + b)
    return counts.takeOrdered(limit, key=lambda row: (-row[1], row[0]))

def main():
    source = input_path("tokens.txt")
    spark = SparkSession.builder.master("local[2]").appName("StationTokens").getOrCreate()
    try:
        emit(top_tokens(spark.sparkContext.textFile(str(source)), 3))
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
