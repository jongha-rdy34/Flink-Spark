| 학습 방향  
사용자의 연속된 활동을 묶어 분석하려면 이벤트 사이의 시간 간격을 고려해야 한다. 
본 실습에서는 사용자별 이벤트 시각을 스트림에 부여한 후, 비활성 간격을 기준으로 세션을 구성한다.
세션 윈도우: EventTimeSessionWindows.with_gap()으로 3초 간격을 지정하고, 같은 세션에 포함된 값들을 reduce()로 합산한다.  

요구사항
1.    실행 환경 생성
   StreamExecutionEnvironment.get_execution_environment()를 사용하여 실행 환경을 생성하세요. 제공된 병렬성 설정을 유지하고 입력부터 결과 출력까지의 처리 흐름을 구성하세요.

2.    입력 데이터 및 이벤트 시각 준비
   제공된 (사용자, 값, 이벤트 시각) 튜플 6개를 사용하세요. user1의 이벤트 시각은 1000, 2000, 4000, 9000, 10000, 12000ms이며 각 값은 1, 3, 4, 9, 5, 7입니다.EventTimestampAssigner에서 세 번째 필드인 element[2]를 이벤트 시각으로 사용하세요. no_watermarks() 설정을 유지하고, 유한 입력 종료 후 창별 결과를 확인하세요.

3.    데이터 스트림 생성
   from_collection()에서 문자열·정수·긴 정수의 튜플 타입을 지정하세요. assign_timestamps_and_watermarks()와 제공된 시각 할당자를 연결하여 이벤트 시간을 부여하세요.

4.    세션 윈도우 집계 처리
   EventTimeSessionWindows.with_gap()의 빈칸을 3초로 채우세요. reduce()에서 값 필드를 합산하고, 4000ms와 9000ms 사이에서 세션이 나뉘는지 확인하세요.

5.    프로그램 실행
   제공된 execute_and_collect()로 작업을 실행하고, 수집한 결과를 반복문과 print()로 출력하세요. 입력 데이터의 처리 결과를 확인하세요.