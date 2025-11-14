# Economy ETL Pipeline

## 프로젝트 개요

본 프로젝트는 다양한 경제 지표를 대상으로 **End-to-End ETL 파이프라인**을 구축하는 것을 목표로 합니다.  
주요 데이터 소스는 다음과 같습니다:

- 주식: NASDAQ, S&P 500 (Yahoo Finance)
- 암호화폐: Bitcoin, 기타 Crypto (Binance API)
- 환율/금리/금: 한국 원-달러, 연준 금리, 국내 금리, 금 (FRED, Yahoo Finance, 한국은행 ECOS)
- 달러 인덱스 / 유가: 글로벌/한국 (Yahoo Finance, OPI Net)

주요 기술 스택:

- **Airflow**: ETL 스케줄링
- **Snowflake**: 데이터 웨어하우스
- **EC2 + Docker**: 배포 환경
- **GitHub Actions**: CI/CD 자동화

본 파이프라인은 각 팀원이 맡은 경제 지표별 데이터 처리 모듈을 개발하고,  
Airflow DAG에서 자동으로 Snowflake에 적재하도록 구성됩니다.  

---
