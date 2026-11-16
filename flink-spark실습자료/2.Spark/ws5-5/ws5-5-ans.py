import csv
from pathlib import Path

from pyspark.sql import SparkSession


# 1. Spark 실행 환경과 파일 경로
spark = (
    SparkSession.builder
    .appName("CustomerChurnAnalysis")
    .getOrCreate()
)
sc = spark.sparkContext
path = Path(__file__).resolve().parent / "data" / "Customer-Churn-Records.csv"


# 2. CSV 문자열을 컬럼 목록으로 변환하는 함수
def parse_row(line):
    return next(csv.reader([line]))


# 3. groupByKey 기반 이탈률 계산
def group_mean(pairs):
    return (
        pairs
        .groupByKey()
        .map(lambda pair: (pair[0], sum(pair[1]) / len(pair[1]) * 100))
    )


# 4. reduceByKey 기반 이탈률 계산
def reduce_mean(pairs):
    return (
        pairs
        .map(lambda pair: (pair[0], (pair[1], 1)))
        .reduceByKey(lambda left, right: (left[0] + right[0], left[1] + right[1]))
        .map(lambda pair: (pair[0], pair[1][0] / pair[1][1] * 100))
    )


# 5. 파일 읽기와 헤더 제거
raw_rdd = sc.textFile(str(path))
print("헤더 포함 처음 5행:", raw_rdd.take(5))
header = raw_rdd.first()
rows = (
    raw_rdd
    .filter(lambda line: line != header)
    .map(parse_row)
)
print("본문 처음 5행:", rows.take(5))


# 6. 신용 점수·연령·카드 유형의 분석 기준
# CSV 열 위치: CreditScore=3, Age=6, Exited=13, Card Type=16
analyses = [
    ("신용 점수대", 3, lambda value: f"{int(value) // 100 * 100}점대"),
    ("연령대", 6, lambda value: f"{int(value) // 10 * 10}대"),
    ("카드 유형", 16, lambda value: value.strip()),
]


# 7. 기준별 두 집계 결과 출력
for label, index, make_key in analyses:
    pairs = (
        rows
        .filter(lambda cols: cols[index].strip() != "" and cols[13].strip() != "")
        .map(lambda cols: (make_key(cols[index]), int(cols[13])))
    )
    grouped = dict(group_mean(pairs).collect())
    reduced = dict(reduce_mean(pairs).collect())
    print(label, "groupByKey:", sorted(grouped.items()))
    print(label, "reduceByKey:", sorted(reduced.items()))


# 8. Spark 세션 종료
spark.stop()
