# 📌 실시간 경제지표 ETL 파이프라인 프로젝트

> Binance API · yfinance · FRED API · Airflow · S3 · Snowflake · Preset Superset  
> 다양한 경제 지표들을 자동 수집 · 정제 · 적재 · 시각화하는 데이터 파이프라인 구축 프로젝트

---

## 📚 목차

- [1. 프로젝트 소개](#1-프로젝트-소개)
- [2. 프로젝트 구성](#2-프로젝트-구성)
- [3. 역할 및 협업 방식](#3-역할-및-협업-방식)
- [4. 개발 과정](#4-개발-과정)
- [5. 프로젝트 폴더 구조](#5-프로젝트-폴더-구조)
- [6. 향후 확장 방향](#6-향후-확장-방향)

---

# 1. 프로젝트 소개

## 1.1 개요

본 프로젝트는 **바이낸스 API, yfinance, FRED API** 등을 활용하여  
가상화폐 가격, 나스닥 및 S&P500 종목, 달러 및 유가 지수, 금리, 환율, 금 가격 등 주요 경제지표를 수집하고  
**각 단계별 전처리 후 S3 → Snowflake → Preset Superset** 으로 전달되는 **ETL 자동화 파이프라인**입니다.

최종적으로 시간별 경제 지표를 시각화하는 **대시보드**를 제공합니다.

## 1.2 프로젝트 목적

- 경제지표 **ETL 파이프라인을 Airflow로 자동화**
- API 기반 **실시간 데이터 수집 시스템 설계**
- **데이터 레이크(S3) & 웨어하우스(Snowflake)** 구축
- **Preset 기반의 시간별 시각화 대시보드 제공**

---

# 2. 프로젝트 구성

## 2.1 프로젝트 아키텍처

<img width="2376" height="946" alt="image" src="https://github.com/user-attachments/assets/ea269aea-c6ce-4ed2-9e92-e0886550b16b" />


## 2.2 데이터 소스

| API | 데이터 종류 |
| --- | --- |
| Binance API | 가상화폐 (BTC 및 주요 알트코인) |
| yfinance | NASDAQ, S&P500, 환율, 유가, 금리, 달러 지수 |
| FRED API | 연준 금리, 국내 금리 |

## 2.3 데이터 레이크 – **AWS S3**

| 폴더명 | 설명 |
| --- | --- |
| `raw-data/` | 원본 데이터 저장 |
| `processed-data/` | 1차 전처리 완료된 CSV 저장 |

## 2.4 데이터 웨어하우스 – **Snowflake**

| 스키마 | 설명 |
| --- | --- |
| `RAW_DATA` | S3 processed → COPY INTO 방식 |
| `ANALYTICS` | 대시보드 / 리포트용 요약 테이블 |

---

# 3. 역할 및 협업 방식

## 3.1 역할 분담

| 이름 | 담당 데이터 | 주요 작업 |
| --- | --- | --- |
| 송여름 | NASDAQ · S&P500 | yfinance / Airflow / Snowflake 적재 / 시각화 |
| 김동영 | 환율 · 금리 · 금 가격 | FRED · yfinance ETL · Snowflake |
| 정수진 | 달러 지수 · 유가 지수 | ETL 모듈 구조화(src 기반) / 테스트 |
| 윤동현 | 가상화폐 | Binance API / ETL 자동화 / 시각화 |

## 3.2 협업 방식

| 툴 | 활용 방식 |
| --- | --- |
| GitHub | 이슈 기반 개발 → 브랜치 전략 → PR 후 dev 병합 |
| Slack | 오류 공유 / 데이터 수집 이슈 해결 / 일정 |
| Zep | API 사용 매뉴얼 및 ETL 흐름 문서화 |
| 정기 회의 | 테이블 구조 / 범위 / 로직 협의 |

---

# 4. 개발 과정

1. **데이터 수집**  
   - yfinance / Binance API / FRED API 활용  
2. **1차 전처리**  
   - 결측치 제거 / 컬럼 정렬 / 소수점 반올림  
3. **S3 적재**  
   - `raw-data` → `processed-data`  
4. **Snowflake 적재**  
   - COPY INTO를 이용한 RAW_DATA 스키마 적재  
5. **Preset Superset 시각화**  
   - 대시보드 구성 (개별 스크린샷은 README에 추가 예정)

---

# 5. 프로젝트 폴더 구조

```bash
economic-etl/
├── .github/                     # GitHub workflow, templates
├── config/                      # 환경변수, connection 설정
├── dags/                        # Airflow DAGs
│   ├── crypto/                  # 가상화폐 관련 ETL DAG 폴더
│   ├── crypto_etl.py                  # 가상화폐 ETL DAG
│   ├── exchange_rate_etl.py           # 환율 ETL DAG
│   ├── gold_price_etl.py              # 금 가격 ETL DAG
│   ├── interest_rate_etl.py           # 금리 ETL DAG
│   ├── load_stocks_to_snowflake.py    # S3 -> Snowflake loading DAG
│   ├── nasdaq_sp500_daily_extract.py  # NASDAQ + S&P500 ETL DAG
│   └── wti_dxy_etl.py
├── deployment/                  # Docker / CI-CD 관련 파일
├── queries/                     # SQL / Airflow 쿼리 모음
│   ├── airflow/
│   └── snowflake/
├── src/                         # ETL 모듈 소스 코드
│   ├── common/                  # 공통 함수 및 유틸리티
│   ├── dollarindex_wti/
│   ├── exchange_interest_gold/
│   ├── nasdaq_sp500/
│   └── __init__.py
├── .env.example                 # 환경변수 예시
├── .gitignore
├── Dockerfile
├── docker-compose.yaml
├── requirements.txt
└── README.md
```
---

# 6. Setup & 실행 가이드

1. 의존성 설치
```bash
pip install -r requirements.txt
```

2. Airflow S3 Connection 설정
   Airflow UI에서 my_s3 AWS Connection 추가

3. Airflow 초기화 & 실행
```bash
airflow db init
airflow scheduler
airflow webserver
```

4. DAG Schedule
```nasdaq_sp500_daily_extract``` DAG: 주중 9 AM KST 실행 (미국 시장 종료 후)

---

# 7. 향후 확장 방향

- dbt 모델링 → 데이터마트 구조로 확장
- Slack Alert → 경제지표들의 변동성 알림
- API Streaming 방식 실험
- Factor & Dimension Table 분리 설계
- 이상치 감지 기반 예측 모델 실험 가능


