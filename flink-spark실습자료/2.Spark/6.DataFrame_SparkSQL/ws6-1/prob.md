| 학습 방향  
●  리스트 형식의 데이터를 사용하여 Spark DataFrame을 코드로 직접 생성합니다.
●  생성된 DataFrame에서 제공하는 기본 연산을 수행하여 데이터 처리 및 변환 과정을 실습합니다.  

요구사항
1.	직원 데이터의 구조와 내용 확인  
    데이터: [ ("Alice", "Data Science", 25), ("Bob", "Machine Learning", 30), ("Charlie", "Data Engineering", 35), ("David", "Data Science", 40), ("Eva", "Machine Learning", 28) ]   컬럼 이름: ["Name", "Department", "Age"]
2.	 제공된 데이터와 컬럼명으로 DataFrame을 생성하고 전체 행을 출력하세요.
3.	부서·나이를 기준으로 직원 정보를 탐색할 입력으로 사용하세요.
4.	Department가 Data Science인 행을 출력하여 특정 부서의 직원을 조회하세요.
5.	Name과 Age만 선택하여 조회에 필요한 컬럼으로 결과를 구성하세요.
6.	Age 내림차순으로 전체 데이터를 출력하여 나이가 많은 직원부터 비교하세요.
7.	Department별 평균 Age를 출력하여 부서별 연령 특성을 비교하세요.
8.	원본 스키마를 출력하여 조회·집계에 사용한 컬럼과 타입을 확인하세요. 