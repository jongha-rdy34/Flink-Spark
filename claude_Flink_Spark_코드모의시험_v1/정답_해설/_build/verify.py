"""정답·응시용을 공지 환경(Java 17 + Python 3.12 + pip의 pyspark 3.5.4 / apache-flink 2.2.1, 클러스터 없음)과
같은 Docker 이미지(fsenv:1)에서 `python 파일.py`로 실행해 검증한다.

사용: python verify.py [p1 p2 ...]
"""
import json
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
IMAGE = "fsenv:1"
ONLY = set(sys.argv[1:])
WORK = Path(tempfile.mkdtemp(prefix="fsv_"))
RESULTS = []


def ok(name, cond, detail=""):
    RESULTS.append((name, bool(cond), str(detail)))
    print(("PASS " if cond else "FAIL ") + name + ("" if cond else f"  :: {str(detail)[:600]}"), flush=True)


def run(rel_dir, script, kind="ans", timeout=300):
    cmd = ["docker", "run", "--rm", "-v", f"{WORK.as_posix()}:/w", "-w", f"/w/{kind}/{rel_dir}",
           IMAGE, "python", script]
    p = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=timeout)
    return p.returncode, p.stdout, p.stderr


def lines_of(out, prefix=None):
    ls = [l.strip() for l in out.splitlines() if l.strip()]
    return [l for l in ls if l.startswith(prefix)] if prefix else ls


def table(out, header):
    rows = []
    for l in lines_of(out):
        if l.startswith("|") and not l.startswith("|--") and header not in l:
            rows.append(",".join(c.strip() for c in l.strip("|").split("|")))
    return rows


def want(q):
    return not ONLY or q in ONLY


def setup():
    shutil.copytree(ROOT / "정답_해설" / "problem", WORK / "ans")
    shutil.copytree(ROOT / "problem", WORK / "exam")
    shutil.copy(HERE / "q3_ui_probe.py", WORK / "q3_ui_probe.py")


def p1():
    rc, out, err = run("problem1", "store_sales.py")
    got = [l for l in lines_of(out) if l.startswith("(")]
    exp = ["('gangnam', 6000)", "('hongdae', 4500)", "('jamsil', 15000)", "('hongdae', 10500)", "('gangnam', 10000)"]
    ok("Q1 정답 실행", rc == 0, err[-400:])
    ok("Q1 누적 결과 5줄(수량 0 제외)", got == exp, got)
    print("    ", got)
    rc, out, err = run("problem1", "store_sales.py", "exam")
    ok("Q1 응시용은 빈칸 때문에 실패", rc != 0 and "___1___" in err, err[-300:])


def p2():
    rc, out, err = run("problem2", "review_wordcount.py")
    got = lines_of(out)
    exp = ["delivery=3", "fast=3", "great=3", "price=3", "packaging=2", "UNIQUE=14"]
    ok("Q2 정답 실행", rc == 0, err[-400:])
    ok("Q2 상위 5개 + UNIQUE=14", got == exp, got)
    print("    ", got)
    rc, out, err = run("problem2", "review_wordcount.py", "exam")
    ok("Q2 응시용은 빈칸 때문에 실패", rc != 0 and "___1___" in err, err[-300:])


def p3():
    for kind in ("ans", "exam"):
        cmd = ["docker", "run", "--rm", "-v", f"{WORK.as_posix()}:/w", "-w", f"/w/{kind}/problem3",
               IMAGE, "python", "/w/q3_ui_probe.py", "lazy_check.py"]
        p = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=300)
        try:
            r = json.loads(p.stdout.strip().splitlines()[-1])
        except Exception:
            ok(f"Q3({kind}) UI 확인 실행", False, p.stdout[-300:] + p.stderr[-300:])
            continue
        ok(f"Q3({kind}) Enter 전 Job 0개, 후 2개(collect·count)",
           r["jobs_before"] == 0 and r["jobs_after"] == 2, r)
        ok(f"Q3({kind}) RESULT [6, 12, 18, 24, 30], COUNT 5",
           any("RESULT [6, 12, 18, 24, 30]" in l for l in r["lines"]) and "COUNT 5" in r["lines"], r["lines"])
        print("    ", r["jobs_before"], "->", r["jobs_after"], r["after_names"])


def p4():
    rc, out, err = run("problem4", "sales_sql.py")
    rows = table(out, "category")
    exp = ["book,2,84000", "beauty,1,43000", "toy,3,43000", "food,2,30000"]
    ok("Q4 정답 실행", rc == 0, err[-400:])
    ok("Q4 DONE만·매출 내림차순·동률 카테고리 오름차순", rows == exp, rows)
    ok("Q4 ROWS=4", "ROWS=4" in out, out[-200:])
    print("    ", rows)
    rc, out, err = run("problem4", "sales_sql.py", "exam")
    ok("Q4 응시용은 NotImplementedError", rc != 0 and "NotImplementedError" in err, err[-300:])


def p5():
    rc, out, err = run("problem5", "sensor_window.py")
    got = sorted(lines_of(out, "WINDOW"))
    exp = sorted(["WINDOW s1 sum=44 last_ts=4999", "WINDOW s2 sum=30 last_ts=2000",
                  "WINDOW s1 sum=25 last_ts=5000", "WINDOW s2 sum=31 last_ts=9999",
                  "WINDOW s1 sum=22 last_ts=12000", "WINDOW s2 sum=29 last_ts=10000"])
    ok("Q5 정답 실행", rc == 0, err[-400:])
    ok("Q5 윈도우 경계(4999|5000, 9999|10000) 결과 6개", got == exp, got)
    ok("Q5 COUNT=6", "COUNT=6" in out, out[-200:])
    print("    출력 순서:", lines_of(out, "WINDOW"))
    rc, out, err = run("problem5", "sensor_window.py", "exam")
    ok("Q5 응시용은 NotImplementedError", rc != 0 and "NotImplementedError" in err, err[-300:])


def p6():
    rc, out, err = run("problem6", "payment_filter.py")
    rows = table(out, "method")
    ok("Q6 정답 실행", rc == 0, err[-400:])
    ok("Q6 수단별 집계", rows == ["card,2,19000", "point,1,9000", "transfer,2,19000"], rows)
    ok("Q6 COUNT=5 TOTAL=47000", "COUNT=5" in out and "TOTAL=47000" in out, out[-200:])
    ok("Q6 amount가 integer로 변환", "amount: integer" in out, out[:400])
    print("    ", rows)
    # 응시용 조건(contains) 그대로 + TODO1만 채운 경우 → UNPAID·REPAID 포함
    bug = WORK / "bug6"
    shutil.copytree(WORK / "ans" / "problem6", bug)
    src = (bug / "payment_filter.py").read_text(encoding="utf-8")
    (bug / "payment_filter.py").write_text(src.replace('col("status") == "PAID"', 'col("status").contains("PAID")'),
                                           encoding="utf-8", newline="\n")
    p = subprocess.run(["docker", "run", "--rm", "-v", f"{WORK.as_posix()}:/w", "-w", "/w/bug6", IMAGE,
                        "python", "payment_filter.py"], capture_output=True, text=True, encoding="utf-8", errors="replace")
    ok("Q6 버그 조건(contains)은 COUNT=8로 틀림", "COUNT=8" in p.stdout, p.stdout[-200:])
    rc, out, err = run("problem6", "payment_filter.py", "exam")
    ok("Q6 응시용은 NotImplementedError", rc != 0 and "NotImplementedError" in err, err[-300:])


def p7():
    rc, out, err = run("problem7", "point_balance.py")
    got = lines_of(out, "BALANCE")
    exp = ["BALANCE u1 EARN -> 500", "BALANCE u2 EARN -> 300", "BALANCE u1 USE -> 300", "BALANCE u2 USE -> 300",
           "BALANCE u1 USE -> 0", "BALANCE u2 EARN -> 400", "BALANCE u2 USE -> 0"]
    ok("Q7 정답 실행", rc == 0, err[-400:])
    ok("Q7 키별 잔액(부족 시 유지, 같으면 사용)", got == exp, got)
    ok("Q7 COUNT=7", "COUNT=7" in out, out[-200:])
    print("    ", got)
    # 채점 입력이 바뀌어도 동작하는지: 다른 입력으로 교체
    alt = WORK / "alt7"
    shutil.copytree(WORK / "ans" / "problem7", alt)
    src = (alt / "point_balance.py").read_text(encoding="utf-8")
    start = src.index("events = [")
    end = src.index("]", start) + 1
    src = src[:start] + 'events = [("a", "USE", 10), ("b", "EARN", 7), ("a", "EARN", 5), ("a", "USE", 5)]' + src[end:]
    (alt / "point_balance.py").write_text(src, encoding="utf-8", newline="\n")
    p = subprocess.run(["docker", "run", "--rm", "-v", f"{WORK.as_posix()}:/w", "-w", "/w/alt7", IMAGE,
                        "python", "point_balance.py"], capture_output=True, text=True, encoding="utf-8", errors="replace")
    ok("Q7 다른 입력(잔액 0에서 사용 → 유지)", lines_of(p.stdout, "BALANCE") ==
       ["BALANCE a USE -> 0", "BALANCE b EARN -> 7", "BALANCE a EARN -> 5", "BALANCE a USE -> 0"], lines_of(p.stdout, "BALANCE"))
    rc, out, err = run("problem7", "point_balance.py", "exam")
    ok("Q7 응시용은 NotImplementedError", rc != 0 and "NotImplementedError" in err, err[-300:])


def main():
    setup()
    print("WORK =", WORK)
    for q, f in [("p1", p1), ("p2", p2), ("p3", p3), ("p4", p4), ("p5", p5), ("p6", p6), ("p7", p7)]:
        if want(q):
            f()
    n = sum(1 for r in RESULTS if r[1])
    print(f"\n==== {n}/{len(RESULTS)} PASS ====")
    for name, good, detail in RESULTS:
        if not good:
            print("FAIL", name, detail[:400])


if __name__ == "__main__":
    main()
