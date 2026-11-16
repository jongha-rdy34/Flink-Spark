"""상품 리뷰에서 자주 등장한 단어를 집계한다.
실행: python review_wordcount.py
"""
# 상황: 리뷰 텍스트 파일에서 불용어를 제외한 단어 빈도를 구하려고 합니다.
# 각 행의 값과 순서는 제공되어 있습니다. ___1___~___5___에 필요한 RDD 메서드 이름만 넣으시오.
# lambda 식·불용어·출력 코드는 바꾸지 않습니다.
from pathlib import Path

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ReviewWordCount").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext

DATA_PATH = Path(__file__).resolve().parent / "data" / "reviews.txt"
stopwords = {"the", "is", "a", "and", "to", "was", "it", "of", "but", "for"}

# 1. 텍스트 파일을 한 줄 = 원소 하나인 RDD로 읽기
lines = sc.⟪___1___‖textFile⟫(str(DATA_PATH))

# 2. 문장을 단어 단위로 펼치기
words = lines.⟪___2___‖flatMap⟫(lambda line: line.split())

# 3. 소문자로 바꾸고 양 끝 문장부호 제거 (원소 하나 → 원소 하나)
cleaned = words.⟪___3___‖map⟫(lambda word: word.lower().strip(".,!?"))

# 4. 두 글자 이상이면서 불용어가 아닌 단어만 남기기
keywords = cleaned.⟪___4___‖filter⟫(lambda word: len(word) >= 2 and word not in stopwords)

# 5. (단어, 1)을 만든 뒤 같은 단어끼리 합산
counts = keywords.map(lambda word: (word, 1)).⟪___5___‖reduceByKey⟫(lambda a, b: a + b)

# 결과 출력: 빈도 내림차순, 같은 빈도는 단어 오름차순으로 상위 5개
top5 = sorted(counts.collect(), key=lambda pair: (-pair[1], pair[0]))[:5]
for word, count in top5:
    print(f"{word}={count}")
print(f"UNIQUE={counts.count()}")

spark.stop()
