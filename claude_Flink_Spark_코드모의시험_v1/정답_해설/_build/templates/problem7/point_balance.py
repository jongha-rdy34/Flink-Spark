"""회원별 포인트 잔액을 키별 상태(ValueState)로 관리한다.
실행: python point_balance.py
"""
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction
from pyflink.datastream.state import ValueStateDescriptor


def apply_point(balance, event_type, amount):
    # TODO 1: 아래 raise를 이벤트 종류에 따라 갱신된 잔액을 반환하는 코드로 교체하시오.
    # EARN: 잔액에 amount를 더한다.
    # USE : 잔액이 amount 이상일 때만 amount를 뺀다. 부족하면 잔액을 그대로 반환한다.
    # 고정 결과 반환은 금지하며, 채점 입력은 변경될 수 있다.
    ⟪raise NotImplementedError("잔액 계산 함수 작성")‖if event_type == "EARN":
        return balance + amount
    if event_type == "USE" and balance >= amount:
        return balance - amount
    return balance⟫


class PointBalance(KeyedProcessFunction):

    def __init__(self):
        self.balance_state = None

    def open(self, runtime_context):
        # TODO 2: 아래 raise를 "balance" 이름의 정수형 ValueStateDescriptor로
        # 키별 상태를 가져와 self.balance_state에 저장하는 코드로 교체하시오.
        ⟪raise NotImplementedError("상태 준비 코드 작성")‖descriptor = ValueStateDescriptor("balance", Types.INT())
        self.balance_state = runtime_context.get_state(descriptor)⟫

    def process_element(self, value, ctx):
        # value: (회원, 이벤트 종류, 포인트)
        # TODO 3: 아래 raise를 다음 네 단계 코드로 교체하시오.
        # 현재 상태를 조회하고, 값이 없으면 0으로 시작한다.
        # apply_point로 새 잔액을 계산한다.
        # 새 잔액으로 상태를 갱신한다.
        # (회원, 이벤트 종류, 새 잔액)을 yield로 내보낸다.
        ⟪raise NotImplementedError("상태 조회·갱신 코드 작성")‖current = self.balance_state.value()
        if current is None:
            current = 0
        new_balance = apply_point(current, value[1], value[2])
        self.balance_state.update(new_balance)
        yield value[0], value[1], new_balance⟫


env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# 입력 데이터: (회원, 이벤트 종류, 포인트)
events = [
    ("u1", "EARN", 500),
    ("u2", "EARN", 300),
    ("u1", "USE", 200),
    ("u2", "USE", 400),
    ("u1", "USE", 300),
    ("u2", "EARN", 100),
    ("u2", "USE", 400),
]

stream = env.from_collection(
    events,
    type_info=Types.TUPLE([Types.STRING(), Types.STRING(), Types.INT()]),
)

balances = (
    stream
    .key_by(lambda x: x[0], key_type=Types.STRING())
    .process(
        PointBalance(),
        output_type=Types.TUPLE([Types.STRING(), Types.STRING(), Types.INT()]),
    )
)

count = 0
with balances.execute_and_collect("Point Balance") as results:
    for value in results:
        print(f"BALANCE {value[0]} {value[1]} -> {value[2]}", flush=True)
        count += 1
print(f"COUNT={count}")
