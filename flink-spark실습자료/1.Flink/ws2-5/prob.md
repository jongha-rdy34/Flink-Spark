| 학습 방향  
실시간 거래를 분석하려면 메시지 입력과 집계 결과의 갱신을 함께 처리하는 것이 중요하다. 
본 실습에서는 Kafka 거래 데이터를 테이블로 읽고 종목별 통계를 계산하여 결과 토픽에 기록한다. 
Kafka 집계: SQL로 거래량 합계와 평균 가격을 계산하고, upsert-kafka를 사용하여 종목 코드와 이름을 키로 결과를 갱신한다.  

요구사항
1.    실행 환경 생성
   StreamExecutionEnvironment와 StreamTableEnvironment를 생성하세요. 제공된 Kafka 커넥터 JAR을 pipeline.jars로 등록하고 Kafka에 접속할 수 있는 환경을 준비하세요.

2.    Kafka 입력 데이터 준비
   stock_trades와 stock_stats 토픽을 준비하고 제공된 kafka_producer.py로 JSON 거래 데이터를 전송하세요. 브로커 주소는 KAFKA_BOOTSTRAP_SERVERS로 지정할 수 있습니다.입력의 주식 코드·이름, 거래 유형, 가격, 거래량, 거래 시각을 소스 스키마와 맞추세요. earliest-offset 설정에 따라 기존 입력도 집계에 포함됨을 확인하세요.

3.    데이터 스트림 생성
   소스 connector는 kafka, 싱크 connector는 upsert-kafka로 채우세요. 싱크의 기본 키 (stock_code, stock_name)과 JSON 키·값 형식을 유지하세요.

4.    종목별 통계 집계
   StatementSet 생성 메서드와 SUM(volume), AVG(price)의 집계 함수 빈칸을 채우세요. 주식 코드와 이름별 결과를 kafka_sink로 기록하세요.

5.    프로그램 실행
   stmt_set.execute()로 작업을 제출하고 JobID를 확인하세요. producer로 입력을 보내고 stock_stats의 키와 값을 소비하여 결과를 확인한 뒤, 실습한 작업을 종료하세요.