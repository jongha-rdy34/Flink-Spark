# TODO: ____를 학습 요구 사항에 맞게 채우세요.
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment

# 실행 환경 생성
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# 정수 목록으로 입력 생성
stream = env.from_collection([1, 2, 3, 4, 5], type_info=Types.INT())

# 각 입력 값을 두 배로 변환
result_stream = stream.____(lambda x: ____, output_type=Types.INT())

# 작업 실행 및 결과 출력
with result_stream.execute_and_collect("Map Example") as results:
    for value in results:
        print(value, flush=True)
