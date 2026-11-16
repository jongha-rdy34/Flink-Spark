"""Q3 검증용(컨테이너 안에서 실행): lazy_check.py를 띄워 Enter 전후 Spark UI의 Job 수를 REST API로 확인한다."""
import json
import subprocess
import sys
import time
import urllib.request

script = sys.argv[1]
proc = subprocess.Popen([sys.executable, script], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                        stderr=subprocess.DEVNULL, text=True, bufsize=1)
lines = []


def read_until(token):
    while True:
        line = proc.stdout.readline()
        if not line:
            return
        lines.append(line.rstrip("\n"))
        if token in line:
            return


def jobs():
    apps = json.load(urllib.request.urlopen("http://localhost:4040/api/v1/applications"))
    app_id = apps[0]["id"]
    return json.load(urllib.request.urlopen(f"http://localhost:4040/api/v1/applications/{app_id}/jobs"))


read_until("Spark UI:")
time.sleep(2)
before = jobs()
proc.stdin.write("\n")
proc.stdin.flush()
read_until("COUNT")
time.sleep(2)
after = jobs()
proc.stdin.write("\n")
proc.stdin.flush()
proc.wait(timeout=60)
print(json.dumps({"lines": lines, "jobs_before": len(before), "jobs_after": len(after),
                  "after_names": sorted(j["name"] for j in after), "rc": proc.returncode}, ensure_ascii=False))
