from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment

from common import emit, flink_rows, input_path

def build(values):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    source = env.from_collection(values, type_info=Types.INT())
    valid = source.filter(lambda weight: weight >= 0)
    return valid.map(lambda weight: weight * 1000, output_type=Types.INT())

def main():
    values = [int(line) for line in input_path("weights.txt").read_text(
        encoding="utf-8").splitlines() if line.strip()]
    emit(flink_rows(build(values), "Q01-weights"))

if __name__ == "__main__":
    main()
