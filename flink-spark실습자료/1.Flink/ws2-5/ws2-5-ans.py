from pathlib import Path
import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

# 실행 환경과 Kafka 커넥터 설정
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
table_env = StreamTableEnvironment.create(env)
kafka_jar = Path(__file__).resolve().with_name("flink-sql-connector-kafka-5.0.0-2.2.jar")
table_env.get_config().get_configuration().set_string("pipeline.jars", kafka_jar.as_uri())

# 주식 거래 Kafka 소스
table_env.execute_sql(f"""
    CREATE TABLE kafka_source (
        stock_code STRING,
        stock_name STRING,
        trade_type STRING,
        price DECIMAL(10,2),
        volume BIGINT,
        trade_time TIMESTAMP(3),
        proctime AS PROCTIME()
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'stock_trades',
        'properties.bootstrap.servers' = '{os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")}',
        'properties.group.id' = 'flink-consumer-group',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json',
        'json.fail-on-missing-field' = 'true'
    )
    """)

# 키별 집계 결과를 갱신하는 Kafka 싱크
table_env.execute_sql(f"""
    CREATE TABLE kafka_sink (
        stock_code STRING,
        stock_name STRING,
        total_volume BIGINT,
        avg_price DECIMAL(10,2),
        update_time TIMESTAMP(3),
        PRIMARY KEY (stock_code, stock_name) NOT ENFORCED
    ) WITH (
        'connector' = 'upsert-kafka',
        'topic' = 'stock_stats',
        'properties.bootstrap.servers' = '{os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")}',
        'key.format' = 'json',
        'value.format' = 'json'
    )
    """)

# 주식 코드·이름별 거래량 합계와 평균 가격
stmt_set = table_env.create_statement_set()
stmt_set.add_insert_sql("""
    INSERT INTO kafka_sink
    SELECT 
        stock_code,
        stock_name,
        SUM(volume) AS total_volume,
        AVG(price) AS avg_price,
        CURRENT_TIMESTAMP as update_time
    FROM kafka_source
    GROUP BY stock_code, stock_name
    """)

# 스트리밍 작업 제출
result = stmt_set.execute()
print("JobID:", result.get_job_client().get_job_id())
