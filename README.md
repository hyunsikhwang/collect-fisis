# 📊 보험사 지급여력비율 분석 대시보드 (Insurance Solvency Dashboard)

금융감독원(FSS) Open API를 통해 국내 보험사의 K-ICS 비율 데이터를 수집하고, 한국은행(BOK) ECOS API의 국고채 금리 데이터와 연동하여 시각화 분석을 제공하는 Streamlit 애플리케이션입니다.

[English Version Below](#english-version)

---

## 🚀 주요 기능

### 1. 데이터 수집 및 캐싱 (MotherDuck)
- **FSS Open API 연동**: 생명보험 및 손해보험사의 지급여력금액, 지급여력기준금액 데이터를 자동으로 수집합니다.
- **MotherDuck 통합**: 수집된 데이터를 클라우드 기반 DuckDB인 MotherDuck에 캐싱하여, 동일한 기준월의 중복 API 호출을 방지하고 빠른 데이터 로딩을 지원합니다.

### 2. K-ICS 비율 분석 대시보드
- **ECharts 기반 시각화**: `pyecharts`를 사용하여 부드럽고 인터랙티브한 시계열 차트를 제공합니다.
- **거시경제 상관성 분석**: 한국은행 국고채 10년물 금리 데이터를 차트에 통합하여 금리 변동과 보험사 건전성 지표간의 관계를 한눈에 파악할 수 있습니다.
- **유동적 스케일**: 데이터의 최소/최대값에 맞춰 Y축 범위를 자동으로 조정하여 변동성을 직관적으로 보여줍니다.

### 3. 사용자 인터페이스 (UI)
- **듀얼 탭 구조**:
    - `📈 분석 대시보드`: 앱 시작 시 자동으로 과거 데이터를 로드하여 트렌드 분석을 즉시 보여줍니다.
    - `📡 데이터 수집기`: 수집 설정(API Key, 기준년월)을 관리하고 새로운 데이터를 획득합니다.
- **반응형 대시보드**: 차트 내 데이터 줌, 범례 토글 등을 통해 상세 분석이 가능합니다.

---

## 🛠 설치 및 시작하기

### 1. 요구 사항
- Python 3.9+
- 금융감독원 Open API Key
- 한국은행 ECOS API Key
- MotherDuck Account & Token

### 2. 라이브러리 설치
```bash
pip install -r requirements.txt
```

### 3. 환경 설정 (`.streamlit/secrets.toml`)
Streamlit Cloud 또는 로컬 환경에서 다음과 같이 비밀 키를 설정해야 합니다.

```toml
FSS_API_KEY = "your_fss_api_key"
ECOS_API_KEY = "your_ecos_api_key"
MOTHERDUCK_TOKEN = "your_motherduck_token"
```

### 4. 앱 실행
```bash
streamlit run app.py
```

---

<a name="english-version"></a>

# 📊 Insurance Solvency Analysis Dashboard

A Streamlit application designed to collect K-ICS ratio data from Korean insurance companies via the FSS Open API and analyze correlations with Treasury bond yields from the BOK ECOS API.

## 🚀 Key Features

### 1. Data Collection & Caching (MotherDuck)
- **FSS Open API Integration**: Automatically retrieves Available Capital and Required Capital data for Life and Non-life insurance companies.
- **MotherDuck Integration**: Caches collected data in MotherDuck (cloud-hosted DuckDB) to prevent redundant API calls and ensure high-performance data loading.

### 2. K-ICS Trend Dashboard
- **ECharts-based Visualization**: Powered by `pyecharts` for smooth, interactive time-series charts.
- **Macroeconomic Correlation**: Synchronizes 10-year Korean Treasury Bond yields on a secondary Y-axis to visualize the relationship between interest rates and insurance solvency.
- **Dynamic Scaling**: Automatically adjusts Y-axis min/max ranges based on the data to highlight subtle trends.

### 3. User Interface (UI)
- **Dual Tab Structure**:
    - `📈 Analysis Dashboard`: Default view that loads historical data instantly for trend analysis.
    - `📡 Data Collector`: Centralized view for managing API credentials and triggering new data captures.
- **Interactive Controls**: Features data zooming, legend toggling (with transitional measures hidden by default), and detailed data previews.

## 🛠 Getting Started

### 1. Requirements
- Python 3.9+
- FSS Open API Key
- BOK ECOS API Key
- MotherDuck Account & Token

### 2. Installation
```bash
pip install -r requirements.txt
```

### 3. Configuration (`.streamlit/secrets.toml`)
Ensure the following credentials are set in your Streamlit secrets:

```toml
FSS_API_KEY = "your_fss_api_key"
ECOS_API_KEY = "your_ecos_api_key"
MOTHERDUCK_TOKEN = "your_motherduck_token"
```

### 4. Running the App
```bash
streamlit run app.py
```