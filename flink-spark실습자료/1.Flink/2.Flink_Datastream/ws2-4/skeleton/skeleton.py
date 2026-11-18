# TODO: ____를 학습 요구 사항에 맞게 채우세요.
from pathlib import Path
import pandas as pd
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment

# 실행 환경 생성
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# CSV에서 주식 티커와 거래 금액 읽기
df = pd.read_csv(Path(__file__).resolve().parents[1] / "data/data.csv")
data = df[["stock_ticker", "amount"]].dropna().values.tolist()
stream = env.from_collection(data, type_info=Types.TUPLE([Types.STRING(), Types.FLOAT()]))

# 금액 5000을 기준으로 분리
high = stream.filter(lambda x: ____)
low = stream.filter(lambda x: ____)

# 낮은 금액에 10%를 한 번 가산한 뒤 병합
adjusted = low.map(lambda x: (x[0], ____),
                   output_type=Types.TUPLE([Types.STRING(), Types.FLOAT()]))
result_stream = high.____(adjusted)

# 작업 실행 및 결과 출력
with result_stream.execute_and_collect("Filter Map Union") as results:
    for value in results:
        print(value, flush=True)
