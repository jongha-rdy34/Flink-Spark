| 학습 방향  
●   Spark 설치 및 환경 설정을 진행합니다.
●   PySpark 실행 및 버전 확인을 진행합니다.  

요구사항
1.		Spark 실행에 필요한 환경 준비
    ○   WSL/Linux에서 Java 17과 Python 3.10 또는 3.11을 준비하세요. 
        두 실행 환경의 버전을 확인한 뒤 설치를 진행하세요.
    ○   Spark 3.5.4 설치 파일을 다운로드하세요.

2.	다운로드한 파일의 압축을 해제하세요.
    설치 폴더를 /home/ssafy/spark로 배치하세요.

3.	터미널에서 Spark를 찾을 수 있도록 환경 변수 설정
    ○   .bashrc에서 JAVA_HOME을 설치된 Java 17 JDK 경로로 설정하세요. 
        아래 SPARK_HOME·PATH와 함께 터미널 실행 환경에 적용하세요.  

    # Spark 환경 변수
    export SPARK_HOME=/home/ssafy/spark
    export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH

4.	  가상환경에서 PySpark 설치 및 실행 확인
    ○   source ~/.bashrc로 환경 변수를 적용하세요.
    ○   자료 루트에서 python3 -m venv .venv로 가상환경을 생성하세요.
    ○   source .venv/bin/activate로 가상환경을 활성화하세요.
        ■   python --version과 python -m pip --version으로 가상환경의   Python과 pip를 사용하는지 확인하세요.
        ■   python -m pip install pyspark==3.5.4로 설치하세요.
        ■   pyspark를 실행하고 spark.version으로 버전을 확인하세요.
        ■   Spark 3.5.4의 버전과 정상 실행 여부가 보이는 화면을 캡처하여 제출하세요.