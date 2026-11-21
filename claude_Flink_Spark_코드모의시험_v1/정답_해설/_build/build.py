"""templates/ 하나로 응시용 problem/ 과 정답 정답_해설/problem/ 을 함께 만든다.

표기: ⟪응시용 텍스트‖정답 텍스트⟫  (여러 줄 가능)
실행: python build.py  (어느 위치에서든 가능, 단 생성 폴더 안에서 실행하지 말 것)
"""
import re
import shutil
from pathlib import Path

HERE = Path(__file__).resolve().parent
TEMPLATES = HERE / "templates"
ROOT = HERE.parent.parent                  # [claude]Flink_Spark_코드모의시험_v1
OUT_EXAM = ROOT / "problem"
OUT_ANS = ROOT / "정답_해설" / "problem"
PAT = re.compile(r"⟪(.*?)‖(.*?)⟫", re.S)


def render(text, answer):
    out = PAT.sub(lambda m: m.group(2) if answer else m.group(1), text)
    assert "⟪" not in out and "⟫" not in out and "‖" not in out, "닫히지 않은 표기"
    return out


def clean(folder):
    if folder.exists():
        for sub in folder.iterdir():
            shutil.rmtree(sub) if sub.is_dir() else sub.unlink()
    folder.mkdir(parents=True, exist_ok=True)


def main():
    clean(OUT_EXAM)
    clean(OUT_ANS)
    n = 0
    for src in sorted(TEMPLATES.rglob("*")):
        if src.is_dir():
            continue
        rel = src.relative_to(TEMPLATES)
        text = src.read_text(encoding="utf-8")
        for out_root, is_ans in ((OUT_EXAM, False), (OUT_ANS, True)):
            dst = out_root / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            with open(dst, "w", encoding="utf-8", newline="\n") as f:
                f.write(render(text, is_ans))
        n += 1
    print(f"rendered {n} files")


if __name__ == "__main__":
    main()
