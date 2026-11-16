# PySpark 실습 팁 노트 — SSAFY 시험 대비

> **범위:** 현재까지 학습한 PySpark RDD, Pair RDD, 파티션, Lazy Evaluation, DataFrame, Spark SQL, CSV·스키마·날짜 변환, JOIN 및 이탈률 집계.  
> **실습 환경:** WSL2 Ubuntu, Python 가상환경(`/home/ssafy/venv`), PySpark 3.5.4, Java 17.  
> **원칙:** 제공된 `skeleton.py`는 문제의 빈칸만 수정. 실행 결과를 확인할 때에는 데이터가 있는 파일의 상대 경로부터 점검.

---

## 0. 시험 직전 1페이지 암기

| 목적 | RDD API | DataFrame API / Spark SQL |
|---|---|---|
| Spark 시작 | `spark = SparkSession.builder.appName("App").getOrCreate()` / `sc = spark.sparkContext` | 동일 |
| 메모리 데이터 생성 | `sc.parallelize(data, 4)` | `spark.createDataFrame(data, columns)` |
| 파일 입력 | `sc.textFile(path)` | `spark.read.option("header", True).option("inferSchema", True).csv(path)` |
| 전체 출력 | `collect()` | `show(truncate=False)` |
| 건수 | `count()` | `count()` / `groupBy().count()` |
| 행 선택 | `filter(lambda x: 조건)` | `filter(col("Age") >= 30)` / `WHERE` |
| 값 변환 | `map(lambda x: ...)` | `select()`, `withColumn()` |
| 단어 펼치기 | `flatMap(lambda line: line.split())` | `explode()` 등 (이번 실습 범위 밖) |
| 키별 집계 | `reduceByKey(lambda a,b:a+b)` | `groupBy().agg(sum(...), avg(...))` |
| 중복 제거 | `distinct()` | `distinct()` |
| 결합 | `union()` / `join()` (Pair RDD) | `join()` / `JOIN` |
| 파티션 수 | `getNumPartitions()` | `rdd.getNumPartitions()` |
| 파티션별 확인 | `glom().collect()` | RDD로 변환 후 확인 |
| 정렬 | `sortByKey()` 또는 `sorted(collect())` | `orderBy(col("x").desc())` / `ORDER BY` |
| SQL 조회 | — | `createOrReplaceTempView("people")`; `spark.sql("SELECT ...")` |

**절대 헷갈리지 말 것**

- **Transformation**: `map`, `filter`, `flatMap`, `mapPartitions`, `union`, `distinct`, `join`, `reduceByKey`, `repartition`, `coalesce` → 새 RDD 정의, 즉시 실행하지 않음.
- **Action**: `collect`, `count`, `take`, `first`, `sum`, `countByKey` → 계산을 실제로 수행함.
- **`groupByKey()` vs `reduceByKey()`**: 전자는 키별 모든 값을 모음, 후자는 **부분 집계(combiner)**가 가능해 합계·개수 계산에 대체로 유리함.
- **`repartition()` vs `coalesce()`**: 전자는 Shuffle 발생, 후자는 기본적으로 Shuffle 없이 파티션 수 감소.
- **`join()`**: 기본 Inner Join. 양쪽에 키가 모두 있어야 결과에 포함.
- **`show()`**: 데이터를 화면에 표시하는 Action. `printSchema()`는 스키마 구조 확인.

---

## 1. 실행 환경·폴더·Spark UI

### 1.1 WSL에서 실행

```bash
source /home/ssafy/venv/bin/activate
python -m pip show pyspark
java -version
cd /실제/실습/폴더
python skeleton.py
```

- `pip show pyspark`에 기대 버전이 없으면 강의 기준 설치: `python -m pip install pyspark==3.5.4`.
- Java가 동작해야 Spark JVM 프로세스를 시작할 수 있음.
- `sc.parallelize()`/`sc.textFile()` 기반 로컬 실습에는 **Flink 클러스터나 Kafka 컨테이너가 필요하지 않음**.
- `spark.stop()`으로 세션 종료. 종료 후에는 Spark UI가 사라질 수 있음.

### 1.2 파일 경로는 항상 문제마다 확인

```python
from pathlib import Path
DATA_DIR = Path(__file__).resolve().parent.parent / "data"  # 부모 폴더의 data
# 또는
DATA_DIR = Path(__file__).resolve().parent / "data"         # skeleton.py 옆의 data
```

```bash
find . -name 'test.txt'
find . -name 'people.csv'
pwd
```

- `Path(__file__).resolve()`는 파일의 절대 경로를 기준으로 하므로 `python skeleton.py`를 어디에서 호출하느냐에 크게 흔들리지 않음.
- 경로가 다르면 **스켈레톤의 경로를 함부로 바꾸기 전에** 실제 프로젝트의 폴더 배치를 확인.

### 1.3 Spark UI와 Lazy Evaluation

- 로컬 Spark UI 기본 주소: **http://localhost:4040** (다른 세션이 점유하면 `4041` 등으로 이동 가능).
- `input("Spark UI 확인 후 Enter: ")`에서 **Enter를 누르기 전에** 브라우저를 연다.
- 원래 코드 앞쪽에 `collect()`가 있었다면 대기 중에도 이미 Job이 보인다. **Enter 이전과 이후의 Jobs 개수 변화**를 비교해야 함.
- WSL에서 포트 확인:

```bash
ss -ltnp | grep -E ':404[0-9]'
curl -I http://localhost:4040
```

- `Connection refused`: 프로그램이 종료됐거나 Spark UI가 다른 포트에서 실행 중/비활성화된 상태.
- UI 설정을 명시해 실행할 필요가 있을 때:

```bash
PYSPARK_SUBMIT_ARGS="--conf spark.ui.enabled=true --conf spark.ui.port=4040 pyspark-shell" python skeleton.py
```

### 1.4 최소 실행 템플릿

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MyPractice").getOrCreate()
sc = spark.sparkContext
print("Spark version:", spark.version)

rdd = sc.parallelize(range(1, 6))
print(rdd.collect())

spark.stop()
```

---

## 2. RDD의 구조: Transformation vs Action

**RDD(Resilient Distributed Dataset)**: 분산된 데이터 원소 집합. 원소는 숫자, 문자열, 튜플 등 가능. `RDD` 변환은 다른 RDD를 만들며 원본을 직접 변경하지 않음.

```python
numbers = sc.parallelize([1, 2, 3, 4, 5])
lazy_rdd = numbers.map(lambda x: x * 2).filter(lambda x: x > 5)
print("정의 완료: 아직 이 체인에 대한 계산 작업을 실행하지 않음")
print(lazy_rdd.collect())  # [6, 8, 10]; 이 시점에 Action으로 계산
```

| 명령 | 구분 | 기억할 점 |
|---|---|---|
| `parallelize(iterable, numSlices=...)` | 생성 | 두 번째 인자로 파티션 수 지정 가능 |
| `textFile(path)` | 생성 | **한 줄 = 원소 하나** |
| `map(fn)` | 변환 | 원소 하나 → 원소 하나 |
| `filter(fn)` | 변환 | True인 원소만 보존 |
| `flatMap(fn)` | 변환 | 원소 하나 → 0개 이상, 펼쳐서 반환 |
| `mapPartitions(fn)` | 변환 | **파티션 iterator**를 함수 입력으로 제공 |
| `collect()` | Action | 전체 결과를 드라이버 메모리로 가져옴 → 대용량에서 주의 |
| `count()` | Action | 원소 개수 |
| `take(5)` | Action | 앞의 5개를 가져옴 (입력 파일 미리보기) |
| `first()` | Action | 첫 번째 원소 (CSV 헤더 확보) |
| `sum()` | Action | 숫자 RDD의 총합 |

### 2.1 세 가지 짝수 2배 변환

```python
rdd = sc.parallelize(range(1, 21), 4)

one = rdd.filter(lambda x: x % 2 == 0).map(lambda x: x * 2)
two = rdd.flatMap(lambda x: [x * 2] if x % 2 == 0 else [])

def transform_partition(iterator):
    return (x * 2 for x in iterator if x % 2 == 0)

three = rdd.mapPartitions(transform_partition)

print(one.collect())
print(two.collect())
print(three.collect())
# 각각 [4, 8, 12, 16, 20, 24, 28, 32, 36, 40]
```

`mapPartitions`의 함수는 원소 하나를 받는 것이 **아니라 파티션마다 iterator 하나**를 받음. 파티션별 연결·초기화 비용 절약 등에 활용.

### 2.2 문자열 필터링 함정

```python
text = sc.textFile(str(DATA_DIR / "test.txt"))
text.filter(lambda s: "data" in s.lower())   # 대소문자 무시, 부분 문자열 포함
text.filter(lambda s: "Spark" in s)           # 대소문자 구분
text.filter(lambda s: s.startswith("Big"))   # 정확한 시작 문자열
text.filter(lambda s: s.endswith("future")) # 정확한 끝 문자열
text.map(lambda s: s.upper())
text.map(lambda s: s.lower())
text.map(lambda s: len(s))  # 띄어쓰기·문장부호도 길이에 포함
```

- `"ai" in line.lower()`는 `"Daily"`의 `ai`까지 잡음. **단어 단위 검사**와 다름.
- 단어 단위 검사 예: `"is" in line.lower().split()` (구두점이 붙으면 별도 처리가 필요).
- `line.split()`은 연속 공백도 편리하게 처리; `line.split(" ")`은 명시적인 스페이스 기준으로 나눔.

---

## 3. 파티션·Shuffle·정렬

### 3.1 파티션 구조 확인

```python
numbers = sc.parallelize(range(1, 101), 4)
n = numbers.getNumPartitions()  # 4
expanded = numbers.repartition(2 * n)  # 8
reduced = expanded.coalesce(n)         # 4

print(numbers.getNumPartitions(), numbers.glom().collect())
print(expanded.getNumPartitions(), expanded.glom().collect())
print(reduced.getNumPartitions(), reduced.glom().collect())
```

- `glom()`: 파티션마다 원소들을 배열로 묶어 반환. `glom().collect()`를 해야 파티션별 데이터가 보임.
- `repartition(n)`: 데이터 Shuffle이 포함된 재분배; 파티션 수 **증가·감소** 모두 가능.
- `coalesce(n)`: 기본적으로 **Shuffle 없이** 파티션을 합침(감소), 불균형이 남을 수 있음.
- 두 연산 모두 데이터 원소를 삭제하지 않음. **순서 보장은 하지 않음**.
- 원소 보존 여부 비교: `sorted(original.collect()) == sorted(modified.collect())` (메모리에 담길 만큼 작을 때).

### 3.2 결합·중복·키 생성

```python
a = sc.parallelize(["Spark", "Kafka", "Spark", "Python"])
b = sc.parallelize(["Kafka", "SQL", "Spark"])
merged = a.union(b)                     # 7개, 중복 유지
unique = merged.distinct()              # 고유 단어 4개
keyed = merged.keyBy(lambda word: word[0])
print(dict(keyed.countByKey()))         # {'S': 4, 'K': 2, 'P': 1}; 키 순서 불특정
```

- `union()` = **결합하며 중복 유지** (SQL의 UNION ALL에 가까움).
- `distinct()` = 중복 제거(Shuffle 가능).
- `keyBy(fn)` = `(fn(원소), 원소)`라는 **Pair RDD** 생성.
- `countByKey()` = 키별 수를 드라이버에 반환하는 **Action**. 키가 지나치게 많으면 주의.

---

## 4. WordCount — RDD 실전 핵심 패턴

```python
from pathlib import Path
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordCount").getOrCreate()
sc = spark.sparkContext
path = Path(__file__).resolve().parent.parent / "data" / "wordCount.txt"

stopwords = {
    "and", "is", "the", "on", "of", "to", "a", "has", "for", "from",
    "that", "this", "it", "an", "be", "are", "by", "in", "with", "as"
}

counts = (
    sc.textFile(str(path))
    .flatMap(lambda line: line.split())
    .map(lambda word: word.lower())
    .filter(lambda word: len(word) >= 2)
    .filter(lambda word: word not in stopwords)
    .map(lambda word: (word, 1))
    .reduceByKey(lambda a, b: a + b)
)
print(counts.collect())
spark.stop()
```

**단계별 기억:** 줄 → 단어 → 소문자 → 2글자 이상 → 불용어 제거 → `(단어,1)` → 키별 합계.

주의: `split()`만으로는 `"spark,"`와 `"spark"`의 문장부호 차이가 정제되지 않음. **시험 스켈레톤에 없는 구두점 처리 임의 추가는 지양**.

---

## 5. Pair RDD 집계·JOIN

### 5.1 `groupByKey()` vs `reduceByKey()`

이탈 여부 `Exited`: 이탈=1, 유지=0. **이탈률(%) = SUM(Exited) / COUNT(*) × 100**.

```python
# pairs 예: [('600점대', 1), ('600점대', 0), ('600점대', 1)]

grouped = (
    pairs.groupByKey()
    .map(lambda p: (p[0], sum(p[1]) / len(p[1]) * 100))
)

reduced = (
    pairs.map(lambda p: (p[0], (p[1], 1)))
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    .map(lambda p: (p[0], p[1][0] / p[1][1] * 100))
)
```

| 관점 | `groupByKey()` | `reduceByKey()` |
|---|---|---|
| 중간 구조 | `key → 값들의 Iterable` | `key → 누적값` |
| 집계 | 값 전체 모은 뒤 평균 | `(이탈합계, 전체건수)` 부분 집계 후 계산 |
| 통신·메모리 | 일반적으로 더 비쌈 | 대개 더 효율적 |
| 추천 | 키별 모든 값 자체가 필요할 때 | 합계, 카운트, 누적 가능 집계 |

### 5.2 고객 이탈률 CSV 실습 구조

```python
import csv
raw = sc.textFile(str(path))
header = raw.first()
rows = raw.filter(lambda line: line != header).map(lambda line: next(csv.reader([line])))

# 실습 파일 기준 인덱스: CreditScore=3, Age=6, Exited=13, Card Type=16
credit_pairs = (
    rows.filter(lambda c: c[3].strip() != "" and c[13].strip() != "")
    .map(lambda c: (f"{int(c[3]) // 100 * 100}점대", int(c[13])))
)
age_pairs = (
    rows.filter(lambda c: c[6].strip() != "" and c[13].strip() != "")
    .map(lambda c: (f"{int(c[6]) // 10 * 10}대", int(c[13])))
)
card_pairs = (
    rows.filter(lambda c: c[16].strip() != "" and c[13].strip() != "")
    .map(lambda c: (c[16].strip(), int(c[13])))
)
```

- `csv.reader`는 단순 `split(',')`와 달리 **인용부호 안의 쉼표**를 고려.
- `sorted(dict(result.collect()).items())`는 **문자열 키 기준 사전식 정렬**. 숫자 크기 정렬과 반드시 같지는 않음.
- 이 예제의 인덱스는 해당 실습 CSV의 열 위치를 전제로 하므로 다른 파일에는 그대로 사용하지 않음.

### 5.3 상품 단가 × 주문 JOIN

```python
prices = sc.parallelize([("A", 1000), ("B", 2000), ("C", 3000)], 2)
orders = sc.parallelize([("A", 2), ("B", 1), ("A", 3), ("C", 2), ("D", 4)], 2)

joined = orders.join(prices)  # (상품, (수량, 단가)); D는 단가 없어서 제외
amounts = joined.map(lambda p: (p[0], p[1][0] * p[1][1]))
totals = amounts.reduceByKey(lambda a, b: a + b)
grand_total = amounts.map(lambda p: p[1]).sum()

print(sorted(joined.collect()))
print(sorted(totals.collect()))  # [('A', 5000), ('B', 2000), ('C', 6000)]
print(grand_total)                # 13000
```

- `orders.join(prices)`는 **Inner Join**: 공통 상품 키만 남음.
- A처럼 주문 2건이면 A 결과도 2건 유지. 원본 5건 → 조인 4건.
- 중첩 튜플: `p[0]`=상품 / `p[1][0]`=수량 / `p[1][1]`=단가.

---

## 6. DataFrame 시작·조회·스키마

### 6.1 생성·선택·정렬·그룹 집계

```python
from pyspark.sql.functions import col, avg, sum, count, when

data = [
    ("Alice", "Data Science", 25, 4000),
    ("Bob", "Machine Learning", 30, 5000),
    ("Charlie", "Data Engineering", 35, 4500),
    ("David", "Data Science", 40, 5500),
    ("Eva", "Machine Learning", 28, 6000),
    ("Frank", "Data Engineering", 32, 4800),
]
df = spark.createDataFrame(data, ["Name", "Department", "Age", "Salary"])

df.show()
df.printSchema()
df.filter(col("Department") == "Data Science").show()
df.select("Name", "Age").show()
df.orderBy(col("Age").desc()).show()

df.groupBy("Department").agg(
    avg("Age").alias("Avg_Age"),
    avg("Salary").alias("Avg_Salary"),
    sum("Salary").alias("Total_Salary"),
    count("*").alias("Employee_Count"),
).show()
```

| 작업 | DataFrame | SQL 개념 |
|---|---|---|
| 컬럼 선택 | `.select("Name", "Age")` | SELECT |
| 조건 | `.filter(col("Age") >= 25)` | WHERE |
| 정렬 | `.orderBy(col("Age").desc())` | ORDER BY Age DESC |
| 그룹화 | `.groupBy("Department")` | GROUP BY |
| 집계 | `.agg(avg("Age"), count("*"))` | AVG, COUNT |
| 새 컬럼 | `.withColumn("Monthly_Salary", col("Salary") / 12)` | SELECT 표현식 |
| 조건부 치환 | `when(조건, "변경값").otherwise(원래값)` | CASE WHEN ... ELSE ... END |

`count("*")`는 모든 행 수를 세고, `count("Age")`는 **Age가 NULL이 아닌 행**만 셈.

### 6.2 새 컬럼 추가 vs 기존 컬럼 교체

```python
df_monthly = df.withColumn("Monthly_Salary", col("Salary") / 12)
df_changed = df_monthly.withColumn(
    "Department",
    when(col("Department") == "Data Science", "AI Research")
    .otherwise(col("Department")),
)

# 반드시 '변환 후' df_changed로 최종 집계
df_changed.groupBy("Department").agg(
    count("*").alias("Num_Employees"),
    avg("Monthly_Salary").alias("Avg_Monthly_Salary"),
).show()
```

- `withColumn("새이름", ...)` = 추가 / `withColumn("기존이름", ...)` = 교체.
- `otherwise(...)` 생략 시 조건 불일치 행에서 NULL이 생길 수 있음.
- `df.filter(...).groupBy(...)`는 **필터된 사람만**, `df_changed.groupBy(...)`는 **변환된 전체 사람**에 대한 집계.

### 6.3 CSV 입력과 명시적 스키마

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Major", StringType(), True),
    StructField("Age", StringType(), True),
])

df = spark.read.csv(str(path), header=False, schema=schema)
df_casted = df.withColumn("Age", col("Age").cast(IntegerType()))
df_casted.printSchema()
df_casted.filter(col("Age") >= 25).select("Name", "Major").show()
df_casted.groupBy("Major").count().show()  # 전체 학생 기준
```

- `StructType`: 전체 구조 / `StructField`: 컬럼 이름, 타입, nullable.
- `header=False`: 파일 첫 줄이 데이터. 지정한 컬럼명을 사용.
- `.option("header", True).option("inferSchema", True).csv(...)`: 헤더 사용 + 타입 추론.
- 타입 추론에만 의존하지 말고 `printSchema()`로 확인.

### 6.4 문자열 → 정수, 날짜

```python
from pyspark.sql.functions import col, to_date
from pyspark.sql.types import IntegerType

df = spark.createDataFrame(
    [("Alice", "25-12-2023", "3500"), ("Bob", "10-01-2024", "4500")],
    ["Customer", "OrderDate", "Price"]
)

df = df.withColumn("Price_Int", col("Price").cast(IntegerType()))
df = df.withColumn("Order_Date_Format", to_date(col("OrderDate"), "dd-MM-yyyy"))
df.printSchema()
df.show(truncate=False)
```

- `"3500"`(string) → `3500`(integer).
- `"25-12-2023"` → `2023-12-25`(date).
- 패턴 **`MM` = 월, `mm` = 분**. 포맷 `dd-MM-yyyy`를 정확히 사용.
- 원본 열과 변환 열을 둘 다 보려면 **새 이름**으로 `withColumn` 호출.

---

## 7. Spark SQL·Temporary View

```python
df.createOrReplaceTempView("employee")
spark.sql("SELECT * FROM employee").show()
spark.sql("SELECT Name, Department FROM employee WHERE Salary >= 5000").show()
spark.sql("SELECT * FROM employee ORDER BY Age DESC").show()
spark.sql("""
    SELECT Department,
           COUNT(*) AS Employee_Count,
           AVG(Salary) AS Avg_Salary
    FROM employee
    GROUP BY Department
""").show()
```

- `createOrReplaceTempView("employee")`: **현재 SparkSession에만 속하는 임시 SQL 뷰**. 영구 저장 아님.
- `spark.sql(...)` 반환값은 **DataFrame**. `show()` Action으로 확인.
- `groupBy(...).agg(...)` ↔ SQL `GROUP BY` + `AVG()/COUNT()/SUM()`.

### 7.1 DataFrame과 SQL로 같은 분석

```python
api_result = (
    people_df
    .filter(col("Age") >= 30)
    .groupBy("Geography")
    .agg(count("*").alias("customer_count"), avg("Age").alias("average_age"))
    .orderBy(col("customer_count").desc(), col("Geography").asc())
)

people_df.createOrReplaceTempView("people")
sql_result = spark.sql("""
    SELECT Geography,
           COUNT(*) AS customer_count,
           AVG(Age) AS average_age
    FROM people
    WHERE Age >= 30
    GROUP BY Geography
    ORDER BY customer_count DESC, Geography ASC
""")
```

- **조건 → 그룹화 → 집계 → 정렬** 논리를 두 방식에 동일하게 적용.
- `ORDER BY customer_count DESC, Geography ASC`: 1차 고객 수 내림차순 / 동률이면 국가명 오름차순.
- CSV 원본은 `spark.read.option("header",True).option("inferSchema",True).csv(path)`.

---

## 8. JOIN 총정리 — RDD와 SQL

### 8.1 SQL JOIN 네 가지

```sql
-- 공통 ID만 (1,2,3)
SELECT COALESCE(e.EmpID, s.EmpID) AS EmpID, e.Name, e.Department, s.Salary
FROM employee e INNER JOIN salary s ON e.EmpID = s.EmpID;

-- 직원 전체 (1,2,3,4): 4번은 급여 NULL
SELECT COALESCE(e.EmpID, s.EmpID) AS EmpID, e.Name, e.Department, s.Salary
FROM employee e LEFT JOIN salary s ON e.EmpID = s.EmpID;

-- 급여 전체 (1,2,3,5): 5번은 직원 정보 NULL
SELECT COALESCE(e.EmpID, s.EmpID) AS EmpID, e.Name, e.Department, s.Salary
FROM employee e RIGHT JOIN salary s ON e.EmpID = s.EmpID;

-- 양쪽 전체 (1,2,3,4,5)
SELECT COALESCE(e.EmpID, s.EmpID) AS EmpID, e.Name, e.Department, s.Salary
FROM employee e FULL OUTER JOIN salary s ON e.EmpID = s.EmpID;
```

| JOIN | 결과 키 (예제) | 의미 |
|---|---|---|
| INNER | 1,2,3 | 양쪽 모두 존재 |
| LEFT | 1,2,3,4 | 왼쪽(employee) 전부 |
| RIGHT | 1,2,3,5 | 오른쪽(salary) 전부 |
| FULL OUTER | 1,2,3,4,5 | 어느 쪽이든 존재 |

```sql
-- 직원 정보 없는 급여만: EmpID=5
SELECT s.EmpID, s.Salary
FROM salary s LEFT JOIN employee e ON s.EmpID = e.EmpID
WHERE e.EmpID IS NULL;

-- 어느 한쪽에만 있는 행: EmpID=4,5
SELECT COALESCE(e.EmpID, s.EmpID) AS EmpID, e.Name, s.Salary
FROM employee e FULL OUTER JOIN salary s ON e.EmpID = s.EmpID
WHERE e.EmpID IS NULL OR s.EmpID IS NULL;
```

- `COALESCE(e.EmpID, s.EmpID)`는 첫 번째 **NULL 아닌 값**을 선택해 한쪽에만 존재하는 키까지 보존.
- SQL의 `NULL` 검사는 `= NULL`이 아닌 **`IS NULL`**.
- `JOIN` 후 출력 순서가 필요하면 SQL `ORDER BY` 또는 RDD `sorted(collect())`를 명시.

---

## 9. 흔한 실수·디버깅 체크리스트

### 실행/경로

- [ ] WSL 터미널에서 가상환경 활성화 (`which python`, `python -m pip show pyspark`)
- [ ] `java -version` 확인
- [ ] `skeleton.py`와 `data/`의 실제 위치 확인 (`find`, `Path(__file__)`)
- [ ] Spark UI는 **실행 중**에만 접근 가능; `input()`에서 Enter 누르기 전 확인
- [ ] 4040 포트 충돌이면 4041 또는 다른 포트 확인

### RDD

- [ ] `map` vs `flatMap`: `map(split)`은 **중첩 리스트** / `flatMap(split)`은 개별 단어
- [ ] `filter(lambda x: ...)` 조건이 **원본에 적용되는지, 변환 결과에 적용되는지** 확인
- [ ] `groupByKey()` 후 값들은 Iterable / `reduceByKey()`는 결합 가능한 집계가 필요
- [ ] `join()`은 기본 Inner Join: 한쪽에만 있는 키는 제외
- [ ] `repartition`은 **순서 불보장**, `coalesce`는 기본 Shuffle 없음
- [ ] `collect()`는 대용량 전체 데이터를 드라이버 메모리에 올리므로 시험의 소규모 입력에서만 편하게 사용

### DataFrame / SQL

- [ ] RDD는 `lambda x`, DataFrame은 `col("...")` 기반 표현식
- [ ] `groupBy` 뒤에 `agg`/`count`로 집계 지정
- [ ] `when(...).otherwise(...)`에서 다른 값 보존
- [ ] `withColumn`에 기존 컬럼명 전달하면 **교체**, 새 컬럼명은 **추가**
- [ ] `Age`/`Price`가 문자열이면 숫자 비교 전에 `cast(IntegerType())`
- [ ] `to_date` 날짜 패턴은 원본 데이터에 맞게 지정 (`dd-MM-yyyy`)
- [ ] `createOrReplaceTempView`를 호출한 **같은 SparkSession**에서 SQL 실행
- [ ] SQL 집계 시 `WHERE`는 `GROUP BY` 이전에 위치
- [ ] `ORDER BY`가 없으면 결과 표시 순서 보장되지 않음

---

## 10. 빈칸 문제 속성 암기표

```text
[RDD]
sc.parallelize(data, partitions)
sc.textFile(str(path))
rdd.map(lambda x: ...)
rdd.filter(lambda x: ...)
rdd.flatMap(lambda x: ...)
rdd.mapPartitions(fn)
rdd.union(other).distinct()
rdd.keyBy(lambda x: x[0]).countByKey()
rdd.map(lambda x: (key, 1)).reduceByKey(lambda a,b:a+b)
orders.join(prices)
rdd.getNumPartitions()
rdd.repartition(8).coalesce(4)
rdd.glom().collect()
rdd.collect() / rdd.count() / rdd.take(5) / rdd.first() / rdd.sum()

[DataFrame]
spark.createDataFrame(data, columns)
spark.read.csv(path, schema=schema, header=False)
spark.read.option('header', True).option('inferSchema', True).csv(path)
df.show(truncate=False) / df.printSchema()
df.filter(col('Age') >= 30).select('Name', 'Age')
df.orderBy(col('Age').desc())
df.groupBy('Department').agg(avg('Age'), count('*'))
df.withColumn('Price_Int', col('Price').cast(IntegerType()))
df.withColumn('Date', to_date(col('OrderDate'), 'dd-MM-yyyy'))
df.withColumn('Department', when(조건, 'AI Research').otherwise(col('Department')))
df.createOrReplaceTempView('employee')
spark.sql('SELECT ... FROM employee WHERE ... GROUP BY ... ORDER BY ...').show()
```

---

## 11. 끝내기 전 자기점검 문제

1. `map(lambda line: line.split())`과 `flatMap(...)`의 출력 구조 차이는?
2. `reduceByKey`가 단순 합계에서 `groupByKey`보다 대체로 효율적인 이유는?
3. 원본 5건 주문 중 단가 없는 D가 `orders.join(prices)`에서 사라지는 이유는?
4. `repartition(8)` 후 `coalesce(4)`에서 어떤 연산이 기본적으로 Shuffle을 수행하는가?
5. `lazy_rdd = rdd.map(...).filter(...)` 정의 직후 Job이 실행되는가? 어떤 호출에서 실행되는가?
6. `df.withColumn('Age', ...)`과 `df.withColumn('Age_Int', ...)`의 차이는?
7. `WHERE Age >= 30`은 국가별 집계 전과 후 중 언제 적용되는가?
8. `FULL OUTER JOIN`에서 한쪽에만 있는 키를 보존하는 함수는?
9. `COUNT(*)`와 `COUNT(Age)`의 NULL 처리 차이는?
10. 문자열 날짜 `25-12-2023`을 DateType으로 변환할 패턴은?

**정답:** ① 중첩 리스트 vs 원소 펼침 ② 파티션별 부분 집계 가능 ③ Inner Join ④ repartition ⑤ Action(`collect` 등) ⑥ 기존 열 교체 vs 새 열 추가 ⑦ 집계 전 ⑧ COALESCE ⑨ 전체 행 vs Age 비-NULL 행 ⑩ `dd-MM-yyyy`.

---

> **노트 사용법:** 0장 암기표 → 2·3·5·6·7·8장 핵심 흐름 → 9장 체크리스트 → 11장 자가 테스트 순으로 복습. 예제의 수치·컬럼 위치는 현재 대화에서 진행한 실습에 맞춰 작성했으며, 새로운 CSV에서는 실제 컬럼과 경로를 다시 확인할 것.
