"""센서별 5초 이벤트 시간 윈도우의 측정값 합계를 구한다.
실행: python sensor_window.py
"""
from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TumblingEventTimeWindows


class EventTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, element, record_timestamp):
        # TODO 1: 아래 raise를 입력 튜플의 세 번째 필드(이벤트 시각 ms)를 반환하는 코드로 교체하시오.
        ⟪raise NotImplementedError("이벤트 시각 추출 코드 작성")‖return element[2]⟫


env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# 입력 데이터: (센서, 측정값, 이벤트 시각 ms)
readings = [
    ("s1", 21, 1000),
    ("s2", 30, 2000),
    ("s1", 23, 4999),
    ("s1", 25, 5000),
    ("s2", 31, 9999),
    ("s2", 29, 10000),
    ("s1", 22, 12000),
]

stream = env.from_collection(
    readings,
    type_info=Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG()]),
)

timed_stream = stream.assign_timestamps_and_watermarks(
    WatermarkStrategy.no_watermarks().with_timestamp_assigner(EventTimestampAssigner())
)

# TODO 2: 아래 raise를 다음 세 단계 코드로 교체하여 window_sums에 저장하시오.
# 센서(0번 필드) 기준으로 그룹화한다.
# 5초 텀블링 이벤트 시간 윈도우를 적용한다.
# reduce로 (센서, 측정값 합계, 두 시각 중 큰 값)을 만든다.
⟪raise NotImplementedError("윈도우 집계 코드 작성")‖window_sums = (
    timed_stream
    .key_by(lambda x: x[0])
    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    .reduce(lambda a, b: (a[0], a[1] + b[1], max(a[2], b[2])))
)⟫

count = 0
with window_sums.execute_and_collect("Sensor Window") as results:
    for value in results:
        print(f"WINDOW {value[0]} sum={value[1]} last_ts={value[2]}", flush=True)
        count += 1
print(f"COUNT={count}")
