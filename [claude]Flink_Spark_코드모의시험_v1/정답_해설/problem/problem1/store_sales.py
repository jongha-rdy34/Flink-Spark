"""매장별 매출 누적 합계를 계산한다.
실행: python store_sales.py
"""
# 상황: 매장 POS에서 들어온 판매 기록으로 매장별 누적 매출을 확인하려고 합니다.
# 각 행의 값과 순서는 제공되어 있습니다. ___1___~___6___에 필요한 메서드 이름만 넣으시오.
# 값·lambda 식·타입 정보는 바꾸지 않습니다.
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment

# 1. 실행 환경 생성
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# 입력 데이터: (매장, 단가, 수량)
sales = [
    ("gangnam", 3000, 2),
    ("hongdae", 4500, 1),
    ("gangnam", 2500, 0),
    ("jamsil", 5000, 3),
    ("hongdae", 3000, 2),
    ("gangnam", 4000, 1),
]

# 2. 리스트를 DataStream으로 변환
stream = env.from_collection(
    sales,
    type_info=Types.TUPLE([Types.STRING(), Types.INT(), Types.INT()]),
)

# 3. 수량이 1 이상인 판매만 선택
valid = stream.filter(lambda x: x[2] >= 1)

# 4. (매장, 단가 × 수량)으로 변환
amounts = valid.map(
    lambda x: (x[0], x[1] * x[2]),
    output_type=Types.TUPLE([Types.STRING(), Types.INT()]),
)

# 5. 매장별로 묶어 금액 누적
totals = amounts.key_by(lambda x: x[0]).sum(1)

# 6. 작업 실행 및 결과 수집
with totals.execute_and_collect("Store Sales") as results:
    for value in results:
        print(value, flush=True)
