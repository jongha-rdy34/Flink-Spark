# TODO: ____를 학습 요구 사항에 맞게 채우세요.
from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TumblingEventTimeWindows

# 입력의 세 번째 필드에서 이벤트 시각(ms) 추출
class EventTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, element, record_timestamp):
        return element[2]

# 실행 환경 생성
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# 입력 데이터: (사용자, 값, 이벤트 시각 ms)
data = [
    ("user1", 1, 1000),
    ("user1", 3, 2000),
    ("user1", 4, 4000),
    ("user1", 9, 9000),
    ("user1", 5, 10000),
    ("user1", 7, 12000),
]

# DataStream 생성
stream = env.from_collection(
    data, type_info=Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG()])
)

# 입력 데이터에 이벤트 시각 할당
timed_stream = stream.assign_timestamps_and_watermarks(
    WatermarkStrategy.no_watermarks().with_timestamp_assigner(EventTimestampAssigner())
)

# 사용자별 5초 이벤트 시간 윈도우 합계
result_stream = (timed_stream.key_by(lambda x: x[0])
                 .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                 .reduce(lambda a, b: (a[0], a[1] + b[1], max(a[2], b[2]))))

# 작업 실행 및 현재 터미널에 결과 출력
with result_stream.execute_and_collect("tumbling_window") as results:
    for value in results:
        print(value, flush=True)
