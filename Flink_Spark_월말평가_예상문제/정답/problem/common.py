import argparse
import json
from pathlib import Path

DATA = Path(__file__).resolve().parent / "data"

def input_path(filename):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DATA / filename)
    return parser.parse_args().input

def emit(result):
    print("RESULT " + json.dumps(result, ensure_ascii=False), flush=True)

def flink_rows(stream, name):
    with stream.execute_and_collect(name) as results:
        return sorted(list(results))
