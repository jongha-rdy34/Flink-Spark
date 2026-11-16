import csv

from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner, WatermarkStrategy
from pyflink.datastream import CheckpointingMode, StreamExecutionEnvironment
from pyflink.datastream.window import TumblingEventTimeWindows

from common import emit, flink_rows, input_path

EVENT_TYPE = Types.TUPLE([Types.STRING(), Types.INT(), Types.LONG()])
RESULT_TYPE = Types.TUPLE([Types.STRING(), Types.LONG(), Types.INT()])
WINDOW_MS = 5000

class EventClock(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return value[2]

def configure_checkpoints(env):
    env.enable_checkpointing(1000, CheckpointingMode.EXACTLY_ONCE)

def build(source):
    timed = source.assign_timestamps_and_watermarks(
        WatermarkStrategy.no_watermarks().with_timestamp_assigner(EventClock())
    )
    summed = (timed.key_by(lambda event: event[0], key_type=Types.STRING())
              .window(TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_MS)))
              .reduce(lambda a, b: (a[0], a[1] + b[1], max(a[2], b[2]))))
    return summed.map(
        lambda row: (row[0], row[2] // WINDOW_MS * WINDOW_MS, row[1]),
        output_type=RESULT_TYPE,
    )

def main():
    with input_path("events.csv").open(encoding="utf-8", newline="") as stream:
        events = [(r["sensor"], int(r["units"]), int(r["event_ms"]))
                  for r in csv.DictReader(stream)]
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    configure_checkpoints(env)
    print("CHECKPOINT", env.get_checkpoint_interval(),
          env.get_checkpointing_consistency_mode().name, flush=True)
    source = env.from_collection(events, type_info=EVENT_TYPE)
    emit(flink_rows(build(source), "Q03-event-window"))

if __name__ == "__main__":
    main()
