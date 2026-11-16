# Flink + Spark 월말평가 모의시험

문제지.md를 열고 problem/ 폴더에서 터미널을 실행합니다. 권장 100분은 환경 준비를 마친 뒤 시작합니다. 각 문항은 독립 실행입니다.

## 환경 준비

공지에 나온 pyspark 3.5.4와 apache-flink 2.2.1을 requirements.txt에 고정했습니다. 연습 기준은 Python 3.10 또는 3.11, JDK 17과 Linux 또는 WSL입니다. JDK는 Python 패키지가 아니므로 사용할 환경에서 java -version과 JAVA_HOME을 확인합니다. 가상환경은 제출 폴더 바깥에 두세요.

```text
python --version
java -version
python -m pip install -r requirements.txt
python -m pip check
python -c "from importlib.metadata import version; print(version('pyspark'), version('apache-flink'))"
```

Java 설치와 가상환경 활성화는 환경별 설정을 따릅니다. Python 패키지가 설치된 인터프리터를 터미널과 IDE에서 동일하게 사용하세요. 이 코드의 Spark는 local[2], Flink는 Python 프로세스가 기동하는 로컬 엔진을 사용합니다. 별도 서버·클러스터 기동은 필요 없습니다. 사전 점검에는 제공 실행만 하는 Q03·Q04를 사용할 수 있습니다.

## 응시와 제출

- 번호 빈칸, TODO, 문제에서 지목한 조건만 수정합니다. 제공 데이터, 실행기, 검사 파일의 이름과 내용은 유지합니다.
- 문제 1~7마다 직접 실행한 화면의 PNG를 각 problemN/ 아래에 저장합니다. 명령과 출력이 함께 읽혀야 합니다. 문제지에 제시한 출력은 비교용 예시입니다.
- 문제 8은 problem8.txt에만 작성합니다. 문제 4의 명령은 problem4/commands.txt에 작성합니다.
- 실행 전후 입력 파일을 수정할 필요가 없습니다. 제출 전 각 파일에 답이 작성됐는지, 각 스크립트가 정상 종료하는지, 해당 PNG가 실제 실행 화면인지 확인하세요.
- problem/ 구조를 유지해 지역_반_이름.zip으로 압축합니다. 가상환경·캐시·엔진 작업 디렉터리는 제외합니다. 이 제출 규칙은 11월 9일 시험을 참고한 모의시험 규칙입니다.

## 결과 검사

```text
python verify.py --quest 3
python verify.py --quest 7 --extra
python verify.py --extra
```

검사기는 실제 로컬 엔진으로 각 스크립트를 띄우고 RESULT와 일부 설정을 비교합니다. --extra는 대체 입력까지 검사합니다. 문항별 실행은 별도 프로세스에서 이뤄져 선행 문항에 영향을 받지 않습니다. Q04 명령 작성과 캡처, Q08 서술형은 직접 확인합니다. 전체 검사는 여러 번 JVM을 시작하므로 수 분 정도 걸릴 수 있습니다.
