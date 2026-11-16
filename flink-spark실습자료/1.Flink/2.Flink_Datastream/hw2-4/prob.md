| 학습 방향  
금융 데이터를 요약하려면 입력 스키마와 집계 지표를 정확하게 정의하는 것이 중요하다. 
본 과제에서는 제공된 CSV를 테이블로 등록한 후, 섹터별 거래 통계를 SQL로 계산하여 출력한다. 
섹터별 요약: GROUP BY sector와 SUM, AVG, COUNT를 사용하여 총 거래액, 평균 가격, 총 거래량, 거래 건수를 계산한다.  

요구사항
1.    실행 환경 생성
   EnvironmentSettings.in_batch_mode()와 TableEnvironment.create()를 사용하여 배치 실행 환경을 생성하세요. 제공된 기본 병렬성 1 설정을 유지하세요.

2.    CSV 데이터 및 스키마 확인
   제공된 data/finance_data.csv를 사용하세요. 헤더 없는 5개 컬럼의 순서는 stock_code, sector, price, volume, transaction_date입니다.컬럼 타입은 순서대로 STRING, STRING, DOUBLE, BIGINT, STRING으로 지정하세요. 총 거래액은 가격과 거래량을 곱한 값의 합계로 계산해야 합니다.

3.    데이터 스트림 생성
   빈칸 (1)에 finance의 CREATE TABLE 문을 작성하세요. filesystem 커넥터, path=input_path, csv 형식과 쉼표 구분자를 지정하여 입력 파일을 연결하세요.

4.    섹터별 금융 통계 집계
   빈칸 (2)에 sector, SUM(price * volume), AVG(price), SUM(volume), COUNT(*) 순서의 SELECT를 작성하세요. FROM finance와 GROUP BY sector를 사용하세요.

5.    프로그램 실행
   result.collect()로 SELECT 결과를 수집하고 제공된 출력 반복문을 사용하세요. row 인덱스와 SELECT 순서를 맞추고, 실수·정수의 출력 형식을 확인하세요.