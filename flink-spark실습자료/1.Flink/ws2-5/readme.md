# WS2-5 | Flink × Kafka 실습 검증
이 파일은 skeleton.py의 빈칸을 작성하고 그 내용을 검증하는 과정을 담고 있습니다.
skeleton.py의 빈칸을 채우고 오세요.

## 1. 실습 목표

Kafka 거래 데이터를 Flink SQL로 집계하고 결과를 Kafka 토픽에 저장한다.

| 항목 | 내용 |
|---|---|
| 입력 토픽 | `stock_trades` |
| 출력 토픽 | `stock_stats` |
| 집계 기준 | `stock_code`, `stock_name` |
| 집계 항목 | `SUM(volume)`, `AVG(price)` |
| 출력 방식 | `upsert-kafka` |

## 2. 실행 및 검증

### STEP 1. Kafka 및 토픽 확인

```bash 
# Kafka 컨테이너 확인
docker ps

# 아무 Kafka 컨테이너를 켜주면 됩니다. 기존에 있으면 Kraft모드, zookeeper-kafka 들어있는거 아무거나 상관 없음. 
# 폴더에 docker-compose 넣어둠. 단, 이걸로 키려면 topic을 만들어놔야함

docker exec ssafy-kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic stock_trades \
  --partitions 1 \
  --replication-factor 1

docker exec ssafy-kafka kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic stock_stats \
  --partitions 1 \
  --replication-factor 1

compose에 있는 컨테이너 이름이 ssafy-kafka고, 이 실습의 producer가 보낼 topic 이름이 각각 stock_trades, stock_stats라서 그걸로 만드는 과정임


# 토픽 목록 확인
docker exec ssafy-kafka kafka-topics \
  --bootstrap-server localhost:9092 --list
```

**확인:** `ssafy-kafka`가 실행 중이고 `stock_trades`, `stock_stats`가 존재해야 한다.

### STEP 2. Flink 작업 실행

```bash
cd ~/flink-exp/data_engineering2_ws_2_5/skeleton

flink run \
  --python skeleton.py \
  --jarfile flink-sql-connector-kafka-5.0.0-2.2.jar
```

작업 상태 확인:

```bash
flink list
```

**확인:** JobID가 출력되고 작업 상태가 `RUNNING`이어야 한다.

Flink Dashboard: http://localhost:8081

### STEP 3. Kafka Producer 실행

새 터미널에서 실행한다.

```bash
cd ~/flink-exp/data_engineering2_ws_2_5/skeleton
source /home/ssafy/venv/bin/activate

# 테스트 거래 10건 전송
KAFKA_EVENT_COUNT=10 python kafka_producer.py
```
### 앞에 count 안붙이고 보내면 1000건의 데이터가 주루륵 보내진다, 정상임

**확인:** `Sent: {...}`가 출력되고 마지막에 `Data generation complete!`가 나타난다.

### STEP 4. 입력 데이터 확인

```bash
docker exec -it ssafy-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic stock_trades \
  --from-beginning
```

**확인:** 주식 코드, 이름, 가격, 거래량 등이 포함된 JSON 메시지가 출력된다.

종료: `Ctrl + C`

### STEP 5. 집계 결과 확인 (핵심)

```bash
docker exec -it ssafy-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic stock_stats \
  --from-beginning \
  --property print.key=true \
  --property key.separator=" | "
```

**예상 출력 예시**

```text
{"stock_code":"005930","stock_name":"삼성전자"} | {"total_volume":100,"avg_price":70000.00,...}

{"stock_code":"005930","stock_name":"삼성전자"} | {"total_volume":300,"avg_price":71000.00,...}
```

실제 값은 랜덤 생성된 거래 데이터에 따라 달라진다.

**검증 기준**

- `stock_code`, `stock_name` 기준으로 종목이 구분되는가?
- `total_volume`이 해당 종목의 거래량 합계인가?
- `avg_price`가 해당 종목의 거래 가격 산술평균인가?
- 새로운 거래 발생 시 집계 결과가 갱신되는가?

> `upsert-kafka`는 동일 키의 결과를 갱신한다. Consumer에서 과거 갱신 메시지도 함께 보일 수 있다.

## 3. 최종 체크리스트

- [ ] Kafka 컨테이너 실행 확인
- [ ] `stock_trades`, `stock_stats` 토픽 확인
- [ ] Flink Job 상태 `RUNNING`
- [ ] Producer 거래 데이터 전송 성공
- [ ] Consumer에서 종목별 집계 결과 확인
- [ ] `SUM(volume)`, `AVG(price)` 정상 계산 확인

## 4. 실습 종료

```bash
# 실행 중인 Flink Job 확인
flink list

# 작업 종료 (실제 JobID 입력)
flink cancel <JobID>
```

Kafka Consumer는 `Ctrl + C`로 종료한다.

필요한 경우 Compose 파일이 있는 디렉터리에서 Kafka를 종료한다.

```bash
docker compose down
```

## 5. 오류 발생 시

| 증상 | 확인 사항 |
|---|---|
| `Connection refused: localhost:8081` | Flink 클러스터 실행 여부 |
| `KafkaTimeoutError` | Kafka 컨테이너, 토픽, 9092 연결 |
| `Cannot discover connector 'kafka'` | Kafka Connector JAR 등록 여부 |
| Job 상태 `FAILED` | Flink Dashboard → Job → Exceptions |
| 집계 결과 없음 | Job `RUNNING` 여부 및 입력 토픽 메시지 확인 |

**최종 성공 기준:** Flink Job이 정상 실행되고 `stock_stats`에서 종목별 거래량 합계와 평균 가격의 갱신 결과가 확인되면 실습 완료.