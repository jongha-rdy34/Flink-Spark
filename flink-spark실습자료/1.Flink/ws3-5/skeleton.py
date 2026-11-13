# TODO: ____를 학습 요구 사항에 맞게 채우세요.
from pyflink.common import Duration
from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.output_tag import OutputTag
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.table import StreamTableEnvironment

# 입력의 세 번째 필드에서 이벤트 시각(ms) 추출
class EventTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, element, record_timestamp):
        return element[2]

# 실행 환경 생성
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# 초당 한 건씩 0~25의 순번 생성
table_env = StreamTableEnvironment.create(env)
table_env.execute_sql("""
    CREATE TEMPORARY TABLE ticks (n BIGINT) WITH (
        'connector' = 'datagen',
        'rows-per-second' = '1',
        'fields.n.kind' = 'sequence',
        'fields.n.start' = '0',
        'fields.n.end' = '25'
    )
""")

ticks = table_env.to_data_stream(table_env.from_path("ticks"))

# 센서 측정값 생성 - 일부 측정값은 과거 이벤트 시각으로 도착
def make_event(row):
    n = row[0]
    timestamp = (n + 1) * 1000
    if n == 7:
        timestamp = 3000
    elif n == 18:
        timestamp = 4000
    return "sensor1", 20 + n, timestamp

stream = ticks.map(make_event, output_type=Types.TUPLE([
    Types.STRING(), Types.INT(), Types.LONG()
]))

# 최대 관측 이벤트 시각에서 1초 지연을 고려한 워터마크 할당
watermarks = (WatermarkStrategy.for_bounded_out_of_orderness(___.of_seconds(1))
              .with_timestamp_assigner(EventTimestampAssigner()))
timed_stream = stream.assign_timestamps_and_watermarks(watermarks)

late_tag = OutputTag("too-late", Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG()]))

# 센서별 5초 창에 3초의 허용 지연 적용
windowed = (timed_stream.___(lambda x: x[0])
            .___(___.of(Time.seconds(5)))
            .___(3000)
            .side_output_late_data(late_tag)
            .reduce(lambda a, b: (a[0], a[1] + b[1], max(a[2], b[2]))))

# 윈도우 결과와 허용 지연 이후 데이터를 구분하여 출력
window_output = windowed.map(lambda x: f"WINDOW: {x}", output_type=Types.STRING())
late_output = windowed.get_side_output(late_tag).map(
    lambda x: f"TOO LATE: {x}", output_type=Types.STRING()
)
result_stream = window_output.union(late_output)

# 작업 실행 및 현재 터미널에 결과 출력
with result_stream.execute_and_collect("Watermark and Late Elements") as results:
    for value in results:
        print(value, flush=True)
