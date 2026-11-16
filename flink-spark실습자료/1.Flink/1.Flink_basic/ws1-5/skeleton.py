# TODO: ____를 학습 요구 사항에 맞게 채우세요.
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment

# 실행 환경 생성
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# 입력 데이터: (키, 값)
data = [("A", 1), ("B", 2), ("A", 3)]
stream = env.from_collection(data, type_info=Types.TUPLE([Types.STRING(), Types.INT()]))
result_stream = stream.key_by(lambda x: x[0]).sum(____)

# 실행 계획의 입력과 집계 연산 확인
print("DAG Execution Plan:\n", env.____())

# 작업 실행 및 결과 출력
with result_stream.execute_and_collect("Execution Plan Example") as results:
    for value in results:
        print(value, flush=True)
