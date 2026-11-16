# 실습 힌트
# - 단가와 수량이 양수인 주문을 filter로 선택합니다.
# - map으로 (주문 ID, 단가 × 수량)을 만들고 제출 터미널에서 확인합니다.

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types


def main():
    # 실행 환경 생성
    env = StreamExecutionEnvironment.____()
    env.set_parallelism(1)

    # (주문 ID, 단가, 수량) 형태의 데이터
    orders = [("A", 12000, 2), ("B", 5000, 1), ("C", 8000, 3), ("D", 10000, 0)]
    order_stream = env.____(
        orders,
        type_info=Types.TUPLE([Types.STRING(), Types.INT(), Types.INT()]),
    )

    # 단가와 수량이 모두 양수인 주문 선택
    valid_orders = order_stream.____(lambda order: order[1] > 0 and order[2] > 0)

    # 주문 ID와 주문 금액 계산
    totals = valid_orders.____(
        lambda order: (order[0], ____),
        output_type=Types.TUPLE([Types.STRING(), Types.INT()]),
    )

    # 작업 실행 및 제출 터미널에 결과 출력
    with totals.____("Order Amount Processing") as results:
        for value in results:
            print(value, flush=True)


if __name__ == "__main__":
    main()
