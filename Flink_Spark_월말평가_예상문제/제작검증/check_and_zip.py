import ast
from collections import Counter, defaultdict
import csv
import json
from pathlib import Path
import sqlite3
from zipfile import ZipFile, ZIP_DEFLATED

BASE = Path(__file__).resolve().parents[1]
areas = {key: BASE / key / "problem" for key in ("응시용", "정답")}
for area, root in areas.items():
    assert (root/"requirements.txt").read_text(encoding="utf-8").splitlines() == [
        "pyspark==3.5.4", "apache-flink==2.2.1"]
    for source in root.rglob("*.py"):
        ast.parse(source.read_text(encoding="utf-8"), filename=str(source), feature_version=(3, 10))
starter, answer = areas.values()
cases = json.loads((answer/"verification_cases.json").read_text(encoding="utf-8"))
def lines(p):
    return p.read_text(encoding="utf-8").splitlines()
def read_csv(p):
    with p.open(encoding="utf-8",newline="") as source:
        return list(csv.DictReader(source))
sql_ast = ast.parse((answer/"problem7/station_report.py").read_text(encoding="utf-8"))
sql = next(n.value for n in ast.walk(sql_ast) if isinstance(n,ast.Constant)
           and isinstance(n.value,str) and "LEFT JOIN readings r" in n.value)
def results(n, flags):
    defaults = {1:"weights.txt",3:"events.csv",5:"status.txt",6:"tokens.txt",7:"readings.csv"}
    p = answer / (flags[1] if flags else "data/"+defaults.get(n,""))
    if n == 1:
        return sorted(int(x)*1000 for x in lines(p) if int(x)>=0)
    if n == 2:
        return {"master":"local[2]","partitions":2,"count":5}
    if n == 3:
        groups = defaultdict(int)
        for r in read_csv(p):
            groups[r["sensor"],int(r["event_ms"])//5000*5000] += int(r["units"])
        return [[k,t,total] for (k,t),total in sorted(groups.items())]
    if n == 4:
        threshold = int(flags[3])
        selected = sorted(int(x) for x in lines(p) if int(x)>=threshold)
        return {"minimum":threshold,"count":len(selected),"values":selected}
    if n in (5,6):
        words = Counter(" ".join(lines(p)).lower().split())
        if n == 5:
            return [[word,k] for word in sorted({"red","blue","green"})
                    for k in range(1,words[word]+1)]
        return [list(pair) for pair in sorted(((w,c) for w,c in words.items()
                                               if w not in {"a","the"}),
                                              key=lambda row:(-row[1],row[0]))[:3]]
    stations = read_csv(answer/"data/stations.csv")
    readings = read_csv(p)
    summaries = []
    for station in stations:
        values = [int(r["units"]) for r in readings if r["station_id"]==station["station_id"]
                  and r["units"] and int(r["units"])>=10]
        summaries.append([station["station_id"],station["zone"],len(values),sum(values)])
    summaries.sort(key=lambda x:(-x[3],x[0]))
    with sqlite3.connect(":memory:") as db:
        db.execute("CREATE TABLE stations(station_id TEXT, zone TEXT)")
        db.execute("CREATE TABLE readings(station_id TEXT, units INTEGER)")
        db.executemany("INSERT INTO stations VALUES(:station_id,:zone)",stations)
        db.executemany("INSERT INTO readings VALUES(?,?)",
                       [(r["station_id"],int(r["units"]) if r["units"] else None) for r in readings])
        sql_rows = [list(row) for row in db.execute(sql)]
    assert summaries == sql_rows
    return {"api":summaries,"sql":sql_rows}
for n,module,flags,expected in cases:
    script=Path(*module.split(".")).with_suffix(".py")
    assert (starter/script).is_file() and (answer/script).is_file()
    for value in flags:
        if value.startswith("data/"): assert (starter/value).is_file()
    assert results(n,flags)==expected,(n,flags,expected,results(n,flags))
print("13 sample and boundary cases independently checked, including 2 SQL executions")
for label,root in areas.items():
    zpath=BASE/(label+".zip")
    with ZipFile(zpath,"w",ZIP_DEFLATED) as z:
        for file in sorted(root.rglob("*")):
            if file.is_file() and "__pycache__" not in file.parts:
                z.write(file,file.relative_to(root.parent))
        if label=="정답": z.write(BASE/"정답"/"해설.md","해설.md")
        for pdf in (BASE/"output/pdf").glob("*.pdf"):
            if label=="응시용": z.write(pdf,pdf.name)
    with ZipFile(zpath) as z:
        assert z.testzip() is None
        assert "problem/requirements.txt" in z.namelist()
        assert "problem/problem8.txt" in z.namelist()
        if label=="응시용":
            assert all("정답" not in name and "해설" not in name for name in z.namelist())
            assert b"NotImplementedError" in z.read("problem/problem5/status_count.py")
            assert b"___1___" in z.read("problem/problem1/weights.py")
        else:
            assert b"NotImplementedError" not in z.read("problem/problem5/status_count.py")
        print(zpath.name,len(z.namelist()),"files")
