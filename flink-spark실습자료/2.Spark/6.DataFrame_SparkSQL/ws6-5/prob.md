| 학습 방향  
●   직원과 급여 DataFrame을 생성하고 SQL 임시 뷰로 등록한다.
●   네 종류 JOIN의 전체 결과를 출력하여 관찰한다.
●   직원 정보가 없는 급여 및 어느 한쪽에만 존재하는 행을 별도로 조회한다.  

요구사항
1. employee 데이터: (1, Alice, Data Science), (2, Bob, Machine Learning), 
                    (3, Charlie, Data Engineering), (4, David, Data Science). 
   컬럼: EmpID, Name, Department.

   ○   salary 데이터: (1, 4000), (2, 5000), (3, 4500), (5, 5500). 
       컬럼: EmpID, Salary.
   
   ○   제공된 두 데이터를 DataFrame과 employee·salary 임시 뷰로 구성하세요. EmpID를 직원과 급여를 연결하는 공통 키로 사용하세요.

2.    INNER·LEFT·RIGHT·FULL OUTER JOIN을 각각 수행하세요. 
      연결되지 않은 직원 또는 급여가 어떤 결과에 포함되는지 비교하세요.
    
    ○   COALESCE(e.EmpID, s.EmpID)를 EmpID로 선택하고 Name·Department·Salary를 출력하세요. 한쪽에만 있는 행의 키도 보존하세요.
    
3.    직원 정보가 없는 급여 행만 별도로 출력하여 대응하는 직원이 없는 데이터를 찾으세요.
4.    양쪽 중 한쪽에만 있는 행을 출력하여 전체 JOIN 결과와 누락 행 조회 결과를 구분하세요.
5.    각 JOIN과 누락 행의 조회 결과를 출력하여 제출하세요.