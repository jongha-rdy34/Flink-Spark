# Apache Flink 실습 핵심 정리 & WSL 환경 트러블슈팅

**기준 환경:** WSL2 Ubuntu / Python 3.10 / PyFlink 2.2.1 / Java 17 / Kafka KRaft

---

## 1. PyFlink 기본 실행 구조

### 1-1. DataStream API

```python
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

stream = env.from_collection(
    [("A", 1), ("B", 2)],
    type_info=Types.TUPLE([Types.STRING(), Types.INT()])
)

with stream.execute_and_collect("Example") as results:
    for value in results:
        print(value, flush=True)
```

| 함수 | 역할 | 핵심 |
|---|---|---|
| `get_execution_environment()` | 실행 환경 생성 | DataStream 시작점 |
| `set_parallelism(n)` | 병렬성 지정 | 병렬 실행 작업 수 |
| `from_collection()` | 리스트 → DataStream | `type_info` 지정 |
| `execute_and_collect()` | 작업 실행 및 결과 수집 | 유한 입력 실습에서 활용 |
| `get_execution_plan()` | 실행 계획 조회 | 실제 데이터 실행과 구분 |

**Key Point**

- PyFlink는 Lazy Evaluation 기반으로 데이터 처리 파이프라인을 구성한다.
- 변환 연산만 정의해서는 작업이 실행되지 않는다.
- `execute_and_collect()`는 실제 작업을 실행한다.
- 병렬성을 높이면 출력 순서는 달라질 수 있다.

### 1-2. 데이터 타입

| PyFlink 타입 | Python 자료형 |
|---|---|
| `Types.STRING()` | `str` |
| `Types.INT()` | `int` (32비트) |
| `Types.LONG()` | `int` (64비트) |
| `Types.FLOAT()` | `float` (32비트) |
| `Types.DOUBLE()` | `float` (64비트) |
| `Types.BOOLEAN()` | `bool` |
| `Types.TUPLE([...])` | 여러 필드로 이루어진 튜플 |

**주의:** Python의 `int`, `float`와 Flink의 Java 기반 타입은 크기·정밀도에 차이가 있다.

---

## 2. DataStream 주요 연산

### 2-1. map / flat_map / filter

| 함수 | 특징 | 사용 예시 |
|---|---|---|
| `map()` | 입력 1개 → 출력 1개 | 금액 계산, 값 변환 |
| `flat_map()` | 입력 1개 → 출력 0개 이상 | 문장 → 단어 목록 |
| `filter()` | 조건에 맞는 데이터만 유지 | 유효 거래 선택 |

```python
# map: 각 값 두 배
stream.map(lambda x: x * 2)

# flat_map: 문장을 단어별로 분리
stream.flat_map(lambda text: text.split())

# filter: 5000 이상만 선택
stream.filter(lambda x: x[1] >= 5000)
```

**Key Point**

- `map()`은 변환.
- `flat_map()`은 펼치기.
- `filter()`는 선택.
- 튜플의 각 필드는 `x[0]`, `x[1]` 등의 인덱스로 접근한다.

### 2-2. key_by / sum / reduce

```python
# 키별 합계
stream.key_by(lambda x: x[0]).sum(1)
```

| 함수 | 역할 |
|---|---|
| `key_by(lambda x: x[0])` | 0번 필드 기준 그룹화 |
| `sum(1)` | 1번 필드 누적 합계 |
| `reduce(lambda a, b: ...)` | 누적 데이터 두 개를 결합 |

복수 필드 누적 예시:

```python
stream.key_by(lambda x: x[0]).reduce(
    lambda a, b: (
        a[0],
        a[1] + b[1],
        a[2] + b[2]
    )
)
```

`(상품, 금액, 건수)` 형태의 스트림이라면 금액과 건수를 동시에 누적한다.

평균 계산:

```python
.map(
    lambda x: (x[0], x[1] / x[2]),
    output_type=Types.TUPLE([
        Types.STRING(),
        Types.FLOAT()
    ])
)
```

**Key Point**

- `sum()`은 특정 숫자 필드의 합계를 계산한다.
- `reduce()`는 여러 필드나 복합 상태를 누적할 때 유용하다.
- 키별 집계는 최종 결과뿐 아니라 중간 누적 결과도 내보낼 수 있다.
- 평균은 `합계 / 건수`로 계산한다.

### 2-3. union

```python
high = stream.filter(lambda x: x[1] >= 5000)
low = stream.filter(lambda x: x[1] < 5000)

adjusted = low.map(
    lambda x: (x[0], x[1] * 1.1),
    output_type=Types.TUPLE([Types.STRING(), Types.FLOAT()])
)

result = high.union(adjusted)
```

**Key Point**

- `union()`은 여러 스트림을 하나로 병합한다.
- 입력 스트림의 데이터 타입이 호환되어야 한다.
- 중복을 자동으로 제거하지 않는다.
- 출력 순서는 보장되지 않는다.

---

## 3. Event Time과 Watermark

### 3-1. 이벤트 시간 지정

```python
from pyflink.common.watermark_strategy import (
    TimestampAssigner,
    WatermarkStrategy
)

class EventTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, element, record_timestamp):
        return element[2]
```

튜플 `(사용자, 값, 이벤트 시각)`에서 세 번째 필드를 이벤트 시간으로 사용한다.

```python
timed_stream = stream.assign_timestamps_and_watermarks(
    WatermarkStrategy.no_watermarks()
    .with_timestamp_assigner(EventTimestampAssigner())
)
```

### 3-2. Watermark 개념

**Watermark는 이벤트 시간이 어디까지 진행되었는지 판단하는 기준이다.**

```python
from pyflink.common import Duration

watermarks = (
    WatermarkStrategy
    .for_bounded_out_of_orderness(Duration.of_seconds(1))
    .with_timestamp_assigner(EventTimestampAssigner())
)
```

| 개념 | 설명 |
|---|---|
| Event Time | 데이터가 실제로 발생한 시각 |
| Processing Time | Flink가 데이터를 처리하는 시각 |
| Watermark | 이벤트 시간 진행 상태를 나타내는 기준 |
| Out-of-Order | 이벤트 발생 순서와 도착 순서가 다른 현상 |
| `no_watermarks()` | 주기적 워터마크를 생성하지 않음 |
| `for_bounded_out_of_orderness()` | 일정 수준의 순서 뒤바뀜을 고려 |

**주의**

- 워터마크의 1초 지연은 실제 데이터 도착을 1초 기다린다는 뜻이 아니다.
- 이벤트 시간 창은 일반적으로 워터마크 진행에 따라 발화한다.
- `no_watermarks()` 상태에서는 스트림 처리 도중 창이 발화하지 않을 수 있다.
- 유한한 소스는 종료 시 최종 워터마크를 전달하여 창 결과를 출력할 수 있다.

---

## 4. Window API 핵심

### 4-1. Tumbling Window

```python
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.time import Time

result = (
    timed_stream
    .key_by(lambda x: x[0])
    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    .reduce(lambda a, b: (
        a[0], a[1] + b[1], max(a[2], b[2])
    ))
)
```

**특징:** 5초 단위의 고정된 창. 서로 겹치지 않는다.

### 4-2. Sliding Window

```python
from pyflink.datastream.window import SlidingEventTimeWindows

.window(
    SlidingEventTimeWindows.of(
        Time.seconds(10),
        Time.seconds(5)
    )
)
```

**특징:** 길이 10초의 창을 5초마다 이동한다.

- 첫 번째 인자: 윈도우 길이(Size)
- 두 번째 인자: 이동 간격(Slide)
- 하나의 이벤트가 여러 창에 포함될 수 있다.

### 4-3. Session Window

```python
from pyflink.datastream.window import EventTimeSessionWindows

.window(
    EventTimeSessionWindows.with_gap(Time.seconds(3))
)
```

**특징:** 같은 키의 이벤트 사이 비활성 간격이 3초 이상이면 세션이 분리된다.

### 4-4. Window 비교

| 유형 | 기준 | 겹침 | 주요 활용 |
|---|---|---|---|
| Tumbling | 고정 길이 | 없음 | 5분 단위 매출 |
| Sliding | 창 길이 + 이동 간격 | 가능 | 최근 10분의 이동 합계 |
| Session | 비활성 간격 | 없음 | 사용자 연속 활동 |
| Global | 전체 데이터를 하나의 창으로 관리 | 별도 트리거 필요 | 개수 기반 집계 |

**시험 핵심:** 시간 창의 분할 방식이 달라지면 같은 데이터의 집계 결과도 달라진다.

---

## 5. Trigger — 개수 기반 집계

### 5-1. Global Window + 사용자 정의 Trigger

```python
from pyflink.datastream.window import GlobalWindows, TriggerResult

result = (
    stream
    .window_all(GlobalWindows.create())
    .trigger(CustomCountTrigger.of(3))
    .reduce(lambda a, b: (
        a[0] + b[0],
        a[1] or b[1]
    ))
)
```

트리거 내부 핵심 조건:

```python
if current_count >= self.count_threshold:
    count_state.clear()
    return TriggerResult.FIRE_AND_PURGE

return TriggerResult.CONTINUE
```

### 5-2. TriggerResult 비교

| 결과 | 동작 |
|---|---|
| `CONTINUE` | 계속 입력 대기 |
| `FIRE` | 결과 출력, 윈도우 내용 유지 |
| `PURGE` | 결과 출력 없이 윈도우 내용 제거 |
| `FIRE_AND_PURGE` | 결과 출력 후 윈도우 내용 제거 |

**Key Point**

- `ValueStateDescriptor`로 누적 입력 개수를 관리한다.
- `count_state.clear()`는 트리거의 카운터를 초기화한다.
- `FIRE_AND_PURGE`는 창의 누적 내용을 제거한다.
- `a[1] or b[1]`은 여러 이벤트 중 하나라도 이상값이면 `True`를 반환한다.

---

## 6. Late Event 처리

### 6-1. 허용 지연과 Side Output

```python
from pyflink.datastream.output_tag import OutputTag

late_tag = OutputTag(
    "too-late",
    Types.TUPLE([
        Types.STRING(), Types.INT(), Types.LONG()
    ])
)

windowed = (
    timed_stream
    .key_by(lambda x: x[0])
    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    .allowed_lateness(3000)
    .side_output_late_data(late_tag)
    .reduce(lambda a, b: (
        a[0], a[1] + b[1], max(a[2], b[2])
    ))
)

late_output = windowed.get_side_output(late_tag)
```

### 6-2. 핵심 함수

| 함수 | 역할 |
|---|---|
| `allowed_lateness(3000)` | 창 종료 후 추가 3000ms의 이벤트 시간 지연 허용 |
| `side_output_late_data(tag)` | 너무 늦은 데이터를 별도 출력으로 분리 |
| `get_side_output(tag)` | Side Output 스트림 가져오기 |

### 6-3. 지연 데이터 사례

5초 창과 3초 허용 지연을 설정한 상황:

- `n=7`: 뒤늦게 도착한 이벤트가 허용 범위 내에 있다면 기존 WINDOW 집계를 갱신한다.
- `n=18`: 워터마크가 창의 정리 시점을 이미 넘었다면 TOO LATE로 분리된다.

**중요:** 실제 판정은 이벤트의 도착 순서만이 아니라, 연산자에 도달한 워터마크를 기준으로 이루어진다. 따라서 스케줄에 따라 실제 중간 출력 결과가 달라질 수 있다.

---

## 7. Table API와 Flink SQL

### 7-1. 배치 실행 환경

```python
from pyflink.table import EnvironmentSettings, TableEnvironment

env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

config = table_env.get_config().get_configuration()
config.set_string("parallelism.default", "1")
```

### 7-2. CSV 파일 테이블 등록

```sql
CREATE TABLE finance (
    stock_code STRING,
    sector STRING,
    price DOUBLE,
    volume BIGINT,
    transaction_date STRING
) WITH (
    'connector' = 'filesystem',
    'path' = '/path/to/finance_data.csv',
    'format' = 'csv',
    'csv.field-delimiter' = ','
)
```

### 7-3. 섹터별 집계 SQL

```sql
SELECT
    sector,
    SUM(price * volume) AS total_amount,
    AVG(price) AS avg_price,
    SUM(volume) AS total_volume,
    COUNT(*) AS transaction_count
FROM finance
GROUP BY sector
```

### 7-4. Table API 핵심 메서드

| 메서드 | 역할 |
|---|---|
| `EnvironmentSettings.in_batch_mode()` | 배치 모드 설정 |
| `TableEnvironment.create()` | Table API 실행 환경 생성 |
| `StreamTableEnvironment.create(env)` | DataStream 환경과 연결된 Table API 생성 |
| `execute_sql()` | SQL DDL/DML/SELECT 수행 |
| `result.collect()` | SELECT 결과 수집 |
| `create_statement_set()` | 여러 INSERT 문을 하나의 작업으로 구성 |
| `stmt_set.add_insert_sql()` | StatementSet에 INSERT SQL 등록 |
| `stmt_set.execute()` | StatementSet 실행 |

**Key Point**

- `SUM(price * volume)`은 총 거래액이다.
- `AVG(price)`는 거래 가격의 산술평균이다.
- `COUNT(*)`는 거래 건수다.
- 배치 SELECT 결과는 `result.collect()`로 수집한다.

---

## 8. Kafka × Flink 연동

### 8-1. 데이터 처리 구조

```text
Kafka Producer
    ↓
stock_trades (Kafka Topic)
    ↓
Flink SQL
    ├── GROUP BY stock_code, stock_name
    ├── SUM(volume)
    └── AVG(price)
    ↓
upsert-kafka
    ↓
stock_stats (Kafka Topic)
    ↓
Kafka Consumer
```

### 8-2. Source / Sink 설정

```sql
-- 입력
'connector' = 'kafka'
'format' = 'json'
'scan.startup.mode' = 'earliest-offset'
```

```sql
-- 출력
'connector' = 'upsert-kafka'
'key.format' = 'json'
'value.format' = 'json'
```

**Key Point**

- Kafka Source: 메시지를 읽는다.
- Upsert Kafka Sink: 동일 기본 키의 최신 집계 결과를 갱신한다.
- `earliest-offset`: 지정된 시작 모드에 따라 가장 오래된 사용 가능한 데이터부터 읽는다.
- `PRIMARY KEY (...) NOT ENFORCED`: 논리적인 고유 키를 선언한다.
- `SUM(volume)`은 총 거래량, `AVG(price)`는 거래 건수 기준 산술평균이다.

### 8-3. 실습 실행 명령어

```bash
# Kafka 확인
docker ps

# 토픽 확인
docker exec ssafy-kafka kafka-topics \
  --bootstrap-server localhost:9092 --list

# Flink 작업 제출
flink run --python skeleton.py \
  --jarfile flink-sql-connector-kafka-5.0.0-2.2.jar

# Flink 작업 상태 확인
flink list

# Producer로 10건 입력
KAFKA_EVENT_COUNT=10 python kafka_producer.py

# 집계 결과 확인
docker exec -it ssafy-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic stock_stats \
  --from-beginning \
  --property print.key=true
```

실행 전 Kafka 브로커 및 토픽과 Flink 클러스터가 준비되어 있어야 한다.

---

# 9. WSL2 + VS Code 개발 환경 설정

## 9-1. Python 및 가상환경

WSL Ubuntu 터미널:

```bash
sudo apt update
sudo apt install -y python3-pip python3-venv
```

가상환경 생성:

```bash
python3 -m venv ~/venv
source ~/venv/bin/activate
```

PyFlink 설치:

```bash
python -m pip install apache-flink==2.2.1
```

설치 확인:

```bash
python -m pip show apache-flink
python -c "import pyflink; print(pyflink.__path__[0])"
```

기존 프로젝트에 가상환경이 이미 있다면 새로 생성하지 않고 재사용한다.

## 9-2. VS Code Python 자동완성이 안 될 때

**가장 흔한 원인: VS Code가 실제 실행하는 Python과 다른 인터프리터를 선택한 경우**

1. VS Code에서 WSL 프로젝트를 연다.
2. Extensions에서 Microsoft **Python** 확장을 설치한다.
3. Microsoft **Pylance** 확장도 설치한다.
4. 확장이 로컬 Windows가 아니라 WSL 환경에 설치되어 있는지 확인한다.
5. `Ctrl + Shift + P` → `Python: Select Interpreter`
6. `Enter interpreter path` → 다음 경로 선택:

```text
/home/ssafy/venv/bin/python
```

자동완성 강제 호출:

```text
Ctrl + Space
```

설정 적용 후:

```text
Ctrl + Shift + P
→ Developer: Reload Window
```

필요하면 `Pylance: Restart Language Server`도 실행한다.

### VS Code settings.json

```json
{
    "python.analysis.autoImportCompletions": true,
    "python.analysis.indexing": true,
    "python.analysis.useLibraryCodeForTypes": true,
    "editor.quickSuggestions": {
        "other": true,
        "comments": false,
        "strings": false
    }
}
```

**주의:** WSL 환경이라면 Python 인터프리터 경로 역시 WSL 내부 경로여야 한다. 패키지를 전역 Python과 venv에 모두 설치해도 자동완성이 보장되는 것은 아니다.

## 9-3. 인터프리터 불일치 확인

```bash
which python
which pip

python -m pip --version
python -m pip show apache-flink
```

예상 경로:

```text
/home/ssafy/venv/bin/python
/home/ssafy/venv/bin/pip
```

VS Code가 선택한 인터프리터와 터미널의 실행 환경을 일치시킨다.

---

# 10. 자주 발생한 오류와 해결 방법

| 오류 | 원인 | 해결 |
|---|---|---|
| `No module named pip` | pip 미설치 | `sudo apt install python3-pip` |
| `No module named pyflink` | 실행 인터프리터에 PyFlink 없음 | venv 확인 후 설치 |
| 자동완성 미작동 | 인터프리터 또는 Pylance 설정 문제 | WSL 확장/인터프리터 확인 |
| `No such file or directory` | 스크립트 경로 오류 | `pwd`, `ls`, `find` 확인 |
| `Cannot discover connector 'kafka'` | Kafka Connector JAR 미인식 | JAR 및 클래스패스 확인 |
| `flink: command not found` | CLI 경로 미등록 | FLINK_HOME/PATH 확인 |
| `localhost:8081 Connection refused` | Flink JobManager 연결 실패 | 클러스터 실행 여부 확인 |
| `KafkaTimeoutError` | 토픽 또는 브로커 연결 문제 | Docker, 토픽, listeners 확인 |
| `FAILED` Job | 실행 중 예외 발생 | Flink Dashboard → Exceptions |
| `GCSFileSystem` import 경고 | 선택적 Google Cloud Storage 의존성 없음 | GCS 미사용 시 대체로 무시 가능 |

### Flink CLI 경로 설정 예시

```bash
export FLINK_HOME=/home/ssafy/venv/lib/python3.10/site-packages/pyflink
export PATH="$FLINK_HOME/bin:$PATH"

flink --version
```

단, `FLINK_HOME`은 실제 Flink 2.2 실행 환경 경로와 일치해야 한다.

### Flink 클러스터 시작

```bash
"$FLINK_HOME/bin/start-cluster.sh"
```

대시보드 확인:

```text
http://localhost:8081
```

### 작업 확인 및 종료

```bash
flink list
flink list -a
flink cancel <JobID>
```

Kafka를 Docker Compose로 실행했다면 Compose 파일이 있는 디렉터리에서:

```bash
docker compose up -d
docker compose down
```

`down -v`는 볼륨 데이터까지 삭제할 수 있으므로 주의한다.

---

# 11. 시험 직전 암기표

| 요구사항 키워드 | 사용할 API |
|---|---|
| 리스트 → 스트림 | `from_collection()` |
| 데이터 하나씩 변환 | `map()` |
| 하나를 여러 개로 분리 | `flat_map()` |
| 조건에 따라 선별 | `filter()` |
| 같은 키끼리 그룹화 | `key_by()` |
| 특정 필드 누적 합계 | `sum(index)` |
| 여러 필드 누적 | `reduce()` |
| 두 스트림 병합 | `union()` |
| 실행 계획 확인 | `get_execution_plan()` |
| 고정 시간 창 | `TumblingEventTimeWindows` |
| 겹치는 시간 창 | `SlidingEventTimeWindows` |
| 비활성 간격 기준 세션 | `EventTimeSessionWindows` |
| 전체 데이터 창 | `GlobalWindows` |
| 사용자 정의 발화 조건 | `trigger()` |
| 발화 후 창 초기화 | `FIRE_AND_PURGE` |
| 이벤트 시각 추출 | `TimestampAssigner` |
| 순서 뒤바뀜 고려 | `for_bounded_out_of_orderness()` |
| 늦은 이벤트 추가 허용 | `allowed_lateness()` |
| 너무 늦은 데이터 분리 | `side_output_late_data()` |
| 사이드 출력 가져오기 | `get_side_output()` |
| SQL로 데이터 집계 | `execute_sql()` |
| SELECT 결과 수집 | `collect()` |
| 여러 INSERT 등록 | `create_statement_set()` |
| Kafka 입력 | `connector = kafka` |
| Kafka 집계 갱신 | `connector = upsert-kafka` |

## 최종 암기 포인트

1. **Map / FlatMap / Filter:** 변환 / 펼침 / 선택
2. **KeyBy / Sum / Reduce:** 그룹화 / 필드 합계 / 사용자 정의 누적
3. **Tumbling / Sliding / Session:** 고정 창 / 겹치는 창 / 비활성 간격 창
4. **Watermark / Allowed Lateness:** 이벤트 시간 진행 판단 / 지연 데이터 추가 허용
5. **FIRE / FIRE_AND_PURGE:** 출력 후 유지 / 출력 후 제거
6. **Kafka / Upsert Kafka:** 메시지 읽기 / 키별 집계 결과 갱신
7. **Python 자동완성:** WSL 확장 + Pylance + venv 인터프리터 일치

---

**정리 기준:** 지금까지 진행한 PyFlink 실습 문제들과 실제 발생한 WSL, Kafka, Flink 실행 오류를 토대로 작성함.