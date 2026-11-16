| 학습 방향  
●   PySpark를 실행합니다.
●   sc.parallelize()를 사용하여 숫자 및 문자열 데이터를 생성 후 기본 연산을     수행합니다.   

요구사항
1.		단어 길이에 따른 데이터 분류
    ○   아래 문자열 목록을 RDD로 생성하세요. 각 원소의 길이를 계산하고 조건에 맞는 단어를 선택할 입력으로 사용하세요.  
        Spark, Parallel, Machine, Learning, Hadoop, Kafka, Big Data  
        
    ○   원본 단어를 출력하세요.
    ○   각 단어의 길이를 출력하세요.
    ○   길이가 6 이상인 단어만 출력하세요.
    ○   해당 단어의 개수를 출력하세요.

2.	같은 숫자 데이터의 조건별 분리
    ○   1~20을 RDD로 생성하고, 같은 원본에서 짝수와 홀수를 각각 선택하세요.
    ○   짝수만 출력하세요.
    ○   홀수만 출력하세요.

3.	대소문자 통일 후 독립 단어 검색
    ○   아래 네 문장을 RDD로 생성하세요.  
        Spark is a powerful analytics engine
        Big Data is transforming industries 
        Data Science is revolutionizing decision making
        Machine Learning and AI are the future  

    ○   map()으로 모든 문장을 소문자로 변환하세요. 
        원문의 대소문자 차이가 검색 조건에 영향을 주지 않도록 구성하세요.
    
    ○   소문자로 변환한 문장을 출력하세요.
    ○   공백으로 구분한 단어 중 is가 있는 문장만 필터링하세요. 
        단어 내부의 부분 문자열과 독립 단어를 구분하여 선택하세요.
    ○   필터링한 문장을 출력하세요. 