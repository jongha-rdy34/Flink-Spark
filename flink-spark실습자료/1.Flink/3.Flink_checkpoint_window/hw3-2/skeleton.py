# TODO: ____를 학습 요구 사항에 맞게 채우세요.
from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows

# 입력의 세 번째 필드에서 이벤트 시각(ms) 추출
class EventTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, element, record_timestamp):
        return element[2]

# 실행 환경 생성
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# 입력 데이터: (사용자, 값, 이벤트 시각 ms)
data = [
    ("A", 2, 1000), ("A", 3, 3000), ("A", 5, 7000),
    ("B", 4, 2000), ("B", 1, 7000), ("B", 6, 9000),
]

# DataStream 생성
stream = env.from_collection(
    data, type_info=Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG()])
)

# 입력 데이터에 이벤트 시각 할당
timed_stream = stream.assign_timestamps_and_watermarks(
    WatermarkStrategy.no_watermarks().with_timestamp_assigner(EventTimestampAssigner())
)

# 같은 입력에 5초 고정 창과 10초·5초 이동 창 적용
keyed = timed_stream.____(lambda x: x[0])
tumbling = (keyed.window(____.of(Time.seconds(5)))
            .____(lambda a, b: (a[0], ____, max(a[2], b[2]))))
sliding = (keyed.window(____.of(Time.seconds(10), Time.seconds(5)))
           .____(lambda a, b: (a[0], ____, max(a[2], b[2]))))

# 두 윈도우의 결과를 구분하여 출력
left = tumbling.map(lambda x: f"TUMBLING: {x}", output_type=Types.STRING())
right = sliding.map(lambda x: f"SLIDING: {x}", output_type=Types.STRING())
result_stream = left.____(right)

# 작업 실행 및 결과 출력
with result_stream.execute_and_collect("Window Comparison") as results:
    for value in results:
        print(value, flush=True)
