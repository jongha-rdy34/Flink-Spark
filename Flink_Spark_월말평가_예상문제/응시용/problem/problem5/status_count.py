from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment

from common import emit, flink_rows, input_path

CODES = {"red", "blue", "green"}
PAIR_TYPE = Types.TUPLE([Types.STRING(), Types.INT()])

def expand(stream):
    # TODO 1: 한 줄을 공백으로 나누고 소문자 코드마다 (코드, 1)을 생성
    raise NotImplementedError("TODO 1")

def tally(pairs):
    allowed = pairs.filter(lambda pair: pair[0] in CODES)
    # TODO 2: 0번 필드로 그룹을 나누고 1번 필드 누적 합산
    raise NotImplementedError("TODO 2")

def main():
    lines = input_path("status.txt").read_text(encoding="utf-8").splitlines()
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    stream = env.from_collection(lines, type_info=Types.STRING())
    emit(flink_rows(tally(expand(stream)), "Q05-status"))

if __name__ == "__main__":
    main()
