# TODO: ____를 학습 요구 사항에 맞게 채우세요.
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment

# 실행 환경 생성
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# 문자열 목록으로 입력 생성
result_stream = env.from_collection(["Hello", "Flink", "World"], type_info=Types.STRING())

# 작업 실행 및 결과 출력
with result_stream.execute_and_collect("Python and CLI Execution") as results:
    for value in results:
        print(value, flush=True)
