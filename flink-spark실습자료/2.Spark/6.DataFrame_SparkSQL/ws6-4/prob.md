| 학습 방향  
●   리스트 데이터로 Spark DataFrame을 생성합니다.
●   DataFrame을 Spark SQL로 조회하기 위해 Temporary View를 생성합니다.
●   Spark SQL의 기본적인 조회 문법과 조건부 필터링, 정렬 및 집계 작업을 단계적으로 학습합니다.  

요구사항
○ 직원 데이터를 SQL 조회 대상으로 구성  
    데이터: [   ("Alice", "Data Science", 25, 4000),   ("Bob", "Machine Learning", 30, 5000),   ("Charlie", "Data Engineering", 35, 4500),   ("David", "Data Science", 40, 5500),   ("Eva", "Machine Learning", 28, 6000),   ("Frank", "Data Engineering", 32, 4800)]   
    컬럼명: ["Name", "Department", "Age", "Salary"]  제공된 데이터와 컬럼명으로 만든 DataFrame을 임시 뷰로 등록하세요.

- 같은 직원을 SQL의 조회 대상으로 사용할 수 있도록 구성하세요.
- 뷰 이름은 employee로 지정하세요.
- SQL로 전체 직원을 출력하여 조회 대상의 원본 내용을 확인하세요.
- Salary가 5000 이상인 직원의 Name과 Department를 출력하여 조건에 맞는 정보만 조회하세요.
- Age 내림차순으로 전체 직원을 출력하여 나이 순서에 따른 조회 결과를 확인하세요.
- Department별 직원 수와 평균 Salary를 출력하여 부서 단위의 규모와 급여를 비교하세요. 