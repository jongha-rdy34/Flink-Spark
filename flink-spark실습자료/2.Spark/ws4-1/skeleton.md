# 실습 힌트

## 목표

Spark 3.5.4를 설치하고 환경 변수를 설정한 뒤, `pyspark` 실행과 버전 확인까지 완료합니다.

## 확인할 내용

- Spark 3.5.4 다운로드 URL을 확인합니다.
- Spark 설치 경로를 `/home/ssafy/spark`로 설정합니다.
- `SPARK_HOME`과 `PATH`를 설정합니다.
- 설치 후 `spark-submit --version` 또는 `pyspark`로 정상 실행 여부를 확인합니다.

## TODO

1. Spark 3.5.4 설치 파일을 다운로드합니다.
2. 압축을 해제합니다.
3. 압축 해제한 Spark 디렉터리를 `/home/ssafy/spark`로 이동합니다.
4. `.bashrc`에 Spark 환경 변수를 추가합니다.
5. 환경 변수를 적용하고 설정값을 확인합니다.
6. Java 17과 Python 버전을 확인하고 자료 루트의 가상환경을 활성화합니다.
7. PySpark 3.5.4를 설치하고 실행합니다.
8. Spark 버전을 확인합니다.

### 1. Apache Spark 3.5.4 다운로드 및 설치

```bash
cd /home/ssafy
wget https://archive.apache.org/dist/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz
tar -xvzf spark-3.5.4-bin-hadoop3.tgz
mv spark-3.5.4-bin-hadoop3 /home/ssafy/spark
```

### 2. 환경 변수 설정

```bash
vi ~/.bashrc
```

`.bashrc` 파일 마지막에 새 터미널에서도 같은 가상환경을 활성화합니다. 설치와 실행 기준은 자료 루트의 spark_setting_guide.md를 따릅니다.

```bash
export SPARK_HOME=/home/ssafy/spark
export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
```

변경 사항을 적용하고 설정값을 확인합니다.

```bash
source ~/.bashrc
echo $SPARK_HOME
```

### 3. 사전 환경 및 실행 준비

```bash
java -version
python3 --version
```

새 터미널에서도 같은 가상환경을 활성화합니다. 설치와 실행 기준은 자료 루트의 spark_setting_guide.md를 따릅니다.

```bash
cd /home/ssafy/e_spark  # 실제 자료 루트 경로
python3 -m venv .venv
source .venv/bin/activate
python --version
python -m pip --version
```

### 4. PySpark 설치 및 버전 확인

```bash
python -m pip install pyspark==3.5.4
pyspark
```

PySpark 쉘에서 Spark 버전을 확인합니다.

```python
spark.version
```
