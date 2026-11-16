# TODO: ____를 학습 요구 사항에 맞게 채우세요.
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import GlobalWindows, Trigger, TriggerResult

# 지정한 개수마다 출력하고 윈도우 내용을 비우는 트리거
class CustomCountTrigger(Trigger):

    def __init__(self, count_threshold):
        super().__init__()
        self.count_threshold = count_threshold

    @staticmethod
    def of(count_threshold):
        return CustomCountTrigger(count_threshold)

    # 입력 개수를 상태에 누적하고 발화 여부 결정
    def on_element(self, element, timestamp, window, ctx):
        count_state_desc = ValueStateDescriptor("count", Types.INT())
        count_state = ctx.get_partitioned_state(count_state_desc)

        current_count = count_state.value() or 0
        current_count += 1
        count_state.update(current_count)

        if current_count >= self.count_threshold:
            count_state.clear()
            return TriggerResult.FIRE_AND_PURGE

        return TriggerResult.CONTINUE

    def on_processing_time(self, time, window, ctx):
        return TriggerResult.CONTINUE

    def on_event_time(self, time, window, ctx):
        return TriggerResult.CONTINUE

    def on_merge(self, window, ctx):
        return None

    # 트리거의 개수 상태 초기화
    def clear(self, window, ctx):
        count_state_desc = ValueStateDescriptor("count", Types.INT())
        ctx.get_partitioned_state(count_state_desc).clear()

# 실행 환경과 입력 생성
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
stream = env.from_collection([1, 3, 4, 9, 5, 7], type_info=Types.INT())

# 각 값에 이상값 여부 표시
marked = stream.map(lambda x: (x, x >= 8),
                    output_type=Types.TUPLE([Types.INT(), Types.BOOLEAN()]))

# 전체 입력 3건마다 합계와 이상값 포함 여부 출력
result_stream = (marked.window_all(GlobalWindows.create())
                 .trigger(CustomCountTrigger.of(3))
                 .reduce(lambda a, b: (a[0] + b[0], a[1] or b[1])))

# 작업 실행 및 결과 출력
with result_stream.execute_and_collect("Custom Trigger and Flag") as results:
    for value in results:
        print(value, flush=True)
