# problem/ — 상세 요구사항·실행 순서

[claude] Flink + Spark 코드베이스 모의시험 v1 (연습용, 실제 시험 아님)

## 환경
- Ubuntu 24.04 / WSL2, Python 3.12, **Java 17**
- `python -m pip install -r requirements.txt` (pyspark==3.5.4, apache-flink==2.2.1)
- Docker·Flink 클러스터·Spark 클러스터를 쓰지 않는다. 모든 문항은 **해당 problemN/ 폴더에서 `python 파일명.py`** 로 실행한다.
- 각 문항은 독립이다. 입력 데이터는 문항 폴더의 `data/` 또는 코드 안에 있다.

## 실행 순서
| 문항 | 수정 파일 | 실행 (problemN/ 안에서) | 제출 캡처 |
|---|---|---|---|
| 01 | store_sales.py | `python store_sales.py` | output1.png |
| 02 | review_wordcount.py | `python review_wordcount.py` | output2.png |
| 03 | (수정 없음) | `python lazy_check.py` → 안내에 따라 Enter 두 번 | output3-1.png, output3-2.png, output3-3.png |
| 04 | sales_sql.py | `python sales_sql.py` | output4.png |
| 05 | sensor_window.py | `python sensor_window.py` | output5.png |
| 06 | payment_filter.py | `python payment_filter.py` | output6-1.png, output6-2.png |
| 07 | point_balance.py | `python point_balance.py` | output7.png |
| 08 | problem8.txt | — | — |

## 코드 작성 규칙
- `___N___` 빈칸에는 메서드 이름만 넣는다. 값·lambda 식·타입 정보는 바꾸지 않는다.
- `raise NotImplementedError(...)` 줄은 작성한 코드로 교체한다. TODO와 문항에서 지정한 오류 코드만 수정한다.
- 제공된 입력 데이터·출력 코드(print, show, COUNT 등)는 바꾸지 않는다.
- Python은 4칸 들여쓰기와 의미 있는 변수명을 사용한다.
- Spark 실행 중 나오는 `WARN NativeCodeLoader` 등의 경고, Flink의 `GCSFileSystem` 관련 경고는 무시해도 된다.

## 제출 (연습용 가정)
- problem/ 폴더 구조를 유지해 `지역_반_이름.zip`으로 압축한다. 가상환경·`__pycache__`·`spark-warehouse` 등은 제외한다.
- 캡처는 각 problemN/ 안에 두며 실행 명령과 출력이 함께 보여야 한다.
- 제출 전 빈 파일이 아닌지, 모든 파일이 실행되는지 다시 확인한다.
