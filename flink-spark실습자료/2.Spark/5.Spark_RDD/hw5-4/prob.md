| 학습 방향  
●   1~20을 4개 파티션의 RDD로 생성한다.
●   세 방식으로 짝수를 2배로 변환한다.
●   각 변환 결과를 collect하여 출력한다.  

요구사항
○ 동일한 변환을 비교할 입력 준비
- 1~20을 4개 파티션의 RDD로 생성하세요. 
- 짝수를 2배로 만드는 처리를 filter+map, flatMap, mapPartitions로 각각 작성하세요.
  
○ filter와 map의 조합
- filter로 짝수를 선택하고 map으로 2배로 변환한 뒤 collect()로 출력하세요.

○ flatMap과 mapPartitions 적용
- flatMap은 짝수일 때 2배 값 하나를, 홀수일 때 빈 목록을 반환하도록 작성하세요.
- mapPartitions는 파티션 iterator의 짝수를 2배로 변환하도록 작성하고 각각 출력하세요.

○ 변환 방식 관찰 
- 각 방식의 함수가 원소와 파티션 중 무엇을 입력받는지 설명하고 spark.stop()으로 종료하세요.