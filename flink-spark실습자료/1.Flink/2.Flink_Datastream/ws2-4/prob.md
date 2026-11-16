| 학습 방향  
데이터의 조건에 따라 서로 다른 처리를 적용하고 결과를 합치는 과정은 스트림 처리에 중요하다. 
본 실습에서는 거래 금액을 기준으로 데이터를 나눈 뒤, 낮은 금액만 보정하여 병합한다. 
분기 및 병합: filter()로 5000을 기준으로 나누고 map()으로 낮은 금액에 10%를 가산한 후, union()으로 결과를 모은다.  

요구사항
1.    실행 환경 생성
   StreamExecutionEnvironment.get_execution_environment()를 사용하여 실행 환경을 생성하세요. 제공된 병렬성 설정을 유지하고 입력부터 결과 출력까지의 처리 흐름을 구성하세요.

2.    CSV 데이터 불러오기 및 전처리
   Pandas의 read_csv()로 제공된 data/data.csv를 읽으세요. stock_ticker와 amount 컬럼을 선택하고 dropna()와 values.tolist()로 전처리하세요.금액이 5000인 데이터는 높은 금액 분기에 포함해야 합니다. 5000 이상과 미만의 조건을 사용하여 모든 유효 입력이 두 분기 중 하나에만 속하도록 구성하세요.

3.    데이터 스트림 생성
   from_collection()과 Types.TUPLE([Types.STRING(), Types.FLOAT()])을 사용하여 티커와 금액의 스트림을 구성하고, 각 분기의 출력 타입을 동일하게 유지하세요.

4.    금액 보정 및 스트림 병합
   두 filter 조건과 low의 map 계산식, 병합 메서드의 빈칸을 채우세요. 낮은 금액에 1.1을 한 번 곱한 뒤 high와 adjusted를 union()으로 합치세요.

5.    프로그램 실행
   제공된 execute_and_collect()로 작업을 실행하고, 수집한 결과를 반복문과 print()로 출력하세요. 입력 데이터의 처리 결과를 확인하세요.  