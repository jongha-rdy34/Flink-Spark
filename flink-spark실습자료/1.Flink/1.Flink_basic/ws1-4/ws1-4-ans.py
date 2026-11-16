# TODO: ____를 학습 요구 사항에 맞게 채우세요.
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment

# 실행 환경 생성
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# 입력 데이터: (과일, 수량)
data = [("apple", 1), ("banana", 1), ("apple", 1), ("banana", 2)]
stream = env.from_collection(data, type_info=Types.TUPLE([Types.STRING(), Types.INT()]))

# 과일별 누적 수량 계산
result_stream = stream.key_by(lambda x: x[0]).sum(1)

# 작업 실행 및 결과 출력
with result_stream.execute_and_collect("Keyed Sum Example") as results:
    for value in results:
        print(value, flush=True)
