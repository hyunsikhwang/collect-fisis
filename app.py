import streamlit as st
import aiohttp
import asyncio
import pandas as pd
import nest_asyncio
import json
import time
import duckdb
import os

# Streamlit 페이지 설정
st.set_page_config(
    page_title="보험사 지급여력비율 수집기",
    page_icon="📊",
    layout="wide"
)

# 비동기 루프 충돌 방지
nest_asyncio.apply()

# ==========================================
# 1. 상수 및 설정 (사이드바 입력)
# ==========================================
st.sidebar.header("⚙️ 설정 (Settings)")

# API 키 (st.secrets 처리)
API_KEY = st.secrets.get("FSS_API_KEY", "")

if not API_KEY:
    API_KEY = st.sidebar.text_input(
        "금융감독원 API Key", 
        type="password",
        help="금융감독원 Open API 인증키를 입력하세요. (계속 사용하시려면 .streamlit/secrets.toml에 FSS_API_KEY를 설정하세요.)"
    )
else:
    st.sidebar.success("✅ API Key가 secrets에서 로드되었습니다.")


TARGET_MONTH = st.sidebar.text_input(
    "기준년월 (YYYYMM)", 
    value="202506",
    help="조회하고 싶은 년월을 입력하세요."
)

TERM = "Q" # 분기
BASE_URL = "http://fisis.fss.or.kr/openapi"
MAX_CONCURRENT_REQUESTS = 20

# ==========================================
# 1.5. MotherDuck DB 설정
# ==========================================
MD_TOKEN = st.secrets.get("MOTHERDUCK_TOKEN", "")
DB_NAME = "fisis_cache"
TABLE_NAME = "insurance_stats"

def get_md_connection():
    """MotherDuck 연결 설정"""
    if not MD_TOKEN:
        return None
    try:
        # MotherDuck 연결 (md: 뒤에 토큰이 없으면 st.secrets에서 가져오거나 환경변수 확인)
        conn = duckdb.connect(f"md:{DB_NAME}?motherduck_token={MD_TOKEN}")
        # 테이블이 없으면 생성
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                구분 VARCHAR,
                회사코드 VARCHAR,
                회사명 VARCHAR,
                계정코드 VARCHAR,
                계정명 VARCHAR,
                기준년월 VARCHAR,
                단위 VARCHAR,
                값 DOUBLE,
                수집일시 TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        return conn
    except Exception as e:
        st.error(f"MotherDuck 연결 오류: {e}")
        return None

def get_cached_data(target_month):
    """MotherDuck에서 기존 데이터 조회"""
    conn = get_md_connection()
    if conn:
        try:
            df = conn.execute(f"SELECT * FROM {TABLE_NAME} WHERE 기준년월 = ?", [target_month]).df()
            conn.close()
            return df
        except Exception as e:
            st.warning(f"데이터 캐시 조회 실패: {e}")
            return pd.DataFrame()
    return pd.DataFrame()

def save_to_md(df):
    """데이터를 MotherDuck에 저장"""
    if df.empty:
        return
    conn = get_md_connection()
    if conn:
        try:
            # 임시 뷰를 생성하여 데이터를 적재
            conn.register("df_to_save", df)
            conn.execute(f"INSERT INTO {TABLE_NAME} SELECT * EXCLUDE(수집일시), CURRENT_TIMESTAMP FROM df_to_save")
            conn.close()
        except Exception as e:
            st.error(f"데이터 저장 실패: {e}")

# ==========================================
# 2. 비동기 통신 함수 정의
# ==========================================
async def fetch_json(session, url, params):
    try:
        async with session.get(url, params=params, timeout=10) as response:
            if response.status == 200:
                text = await response.text()
                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    return None
            else:
                return None
    except Exception:
        return None

async def get_companies(session, part_div):
    """금융회사 코드 조회"""
    url = f"{BASE_URL}/companySearch.json"
    params = {"lang": "kr", "auth": API_KEY, "partDiv": part_div}
    data = await fetch_json(session, url, params)

    company_list = []
    if data and 'result' in data and 'list' in data['result']:
        for item in data['result']['list']:
            company_list.append({
                'financeCd': item['finance_cd'],
                'financeNm': item['finance_nm'],
                'partDiv': part_div
            })
    return company_list

async def get_accounts(session, list_no):
    """계정항목 조회"""
    url = f"{BASE_URL}/accountListSearch.json"
    params = {"lang": "kr", "auth": API_KEY, "listNo": list_no}
    data = await fetch_json(session, url, params)

    account_list = []
    if data and 'result' in data and 'list' in data['result']:
        for item in data['result']['list']:
            account_list.append({
                'accountCd': item['account_cd'],
                'accountNm': item['account_nm'],
                'listNo': list_no
            })
    return account_list

async def fetch_statistics(session, semaphore, company, account, pbar, status_text):
    """통계정보 수집"""
    url = f"{BASE_URL}/statisticsInfoSearch.json"
    params = {
        "lang": "kr",
        "auth": API_KEY,
        "financeCd": company['financeCd'],
        "listNo": account['listNo'],
        "accountCd": account['accountCd'],
        "term": TERM,
        "startBaseMm": TARGET_MONTH,
        "endBaseMm": TARGET_MONTH
    }

    async with semaphore:
        data = await fetch_json(session, url, params)
    
    # 진행률 업데이트 (UI) - 너무 잦은 업데이트는 성능 저하를 유발하므로 주의
    # 여기서는 간단히 로직만 수행하고 결과 반환

    if data and 'result' in data and 'list' in data['result']:
        result_list = data['result']['list']
        if result_list:
            item = result_list[0]
            # 값 우선순위 확인
            raw_value = item.get('a') or item.get('won') or item.get('column_value') or 0

            return {
                '구분': '생명보험' if company['partDiv'] == 'H' else '손해보험',
                '회사코드': company['financeCd'],
                '회사명': company['financeNm'],
                '계정코드': account['accountCd'],
                '계정명': account['accountNm'],
                '기준년월': item.get('base_month', TARGET_MONTH),
                '단위': item.get('unit_name', ''),
                '값': raw_value
            }
    return None

# ==========================================
# 3. 메인 실행 로직 (Async Wrapper)
# ==========================================
async def run_async_collection():
    status_container = st.status("🚀 데이터 수집 및 캐시 확인 중...", expanded=True)
    
    try:
        # 0. MotherDuck 캐시 확인
        status_container.write(f"🔎 {TARGET_MONTH} 데이터 캐시 확인 중...")
        cached_df = get_cached_data(TARGET_MONTH)
        
        if not cached_df.empty:
            status_container.write(f"✅ {len(cached_df)}건의 데이터를 MotherDuck에서 로드했습니다.")
        
        async with aiohttp.ClientSession() as session:
            # 1. 목록 조회
            status_container.write("🔍 1. 금융회사 및 계정항목 목록 조회 중...")
            
            # 병렬로 목록 가져오기
            f1 = get_companies(session, 'H')
            f2 = get_companies(session, 'I')
            f3 = get_accounts(session, 'SH021')
            f4 = get_accounts(session, 'SI021')
            
            life_companies, non_life_companies, life_accounts, non_life_accounts = await asyncio.gather(f1, f2, f3, f4)
            
            total_companies = len(life_companies) + len(non_life_companies)
            status_container.write(f"✅ 회사 목록 확보: 총 {total_companies}개")

            # 2. 작업 생성 (캐시에 없는 것만)
            tasks = []
            semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
            
            status_container.write("📦 2. 미수집 데이터 확인 및 요청 생성 중...")
            
            # 기존 데이터 키 생성 (회사코드, 계정코드)
            existing_keys = set()
            if not cached_df.empty:
                existing_keys = set(zip(cached_df['회사코드'], cached_df['계정코드']))

            def build_tasks(companies, accounts):
                for comp in companies:
                    for acc in accounts:
                        if (comp['financeCd'], acc['accountCd']) not in existing_keys:
                            tasks.append(fetch_statistics(session, semaphore, comp, acc, None, None))

            build_tasks(life_companies, life_accounts)
            build_tasks(non_life_companies, non_life_accounts)

            total_tasks = len(tasks)
            
            if total_tasks == 0:
                status_container.write("✨ 모든 데이터가 이미 캐시되어 있습니다.")
                status_container.update(label="✅ 캐시 데이터 리로드 완료!", state="complete", expanded=False)
                return cached_df.to_dict('records')

            status_container.write(f"📡 총 {total_tasks} 건의 새로운 데이터를 API로 수집합니다...")

            # 3. 실행 및 진행률 표시
            new_results = []
            progress_bar = status_container.progress(0)
            completed_count = 0
            
            for f in asyncio.as_completed(tasks):
                res = await f
                if res:
                    new_results.append(res)
                
                completed_count += 1
                if total_tasks > 0:
                    progress_bar.progress(completed_count / total_tasks)

            # 4. 새로운 데이터 DB 저장
            if new_results:
                status_container.write(f"💾 {len(new_results)}건의 새로운 데이터를 MotherDuck에 저장 중...")
                new_df = pd.DataFrame(new_results)
                # 값 전처리 (저장 전 숫자로 변환)
                new_df['값'] = pd.to_numeric(new_df['값'].astype(str).str.replace(',', ''), errors='coerce')
                save_to_md(new_df)
                
                # 기존 데이터와 합치기
                if not cached_df.empty:
                    # 수집일시 컬럼 제외하고 합치기 (cached_df에는 수집일시가 있을 수 있음)
                    cols = ['구분', '회사코드', '회사명', '계정코드', '계정명', '기준년월', '단위', '값']
                    all_results_df = pd.concat([cached_df[cols], new_df[cols]], ignore_index=True)
                    results = all_results_df.to_dict('records')
                else:
                    results = new_results
            else:
                results = cached_df.to_dict('records')

            status_container.update(label="✅ 데이터 수집 및 캐싱 완료!", state="complete", expanded=False)
            return results

    except Exception as e:
        status_container.update(label="⚠️ 오류 발생", state="error")
        st.error(f"오류 상세: {e}")
        return []

# ==========================================
# 4. Streamlit UI 구성
# ==========================================
st.title("📊 보험사 지급여력비율 조회기")
st.markdown(f"""
금융감독원 Open API를 사용하여 보험사의 지급여력비율 관련 데이터를 수집합니다.
- **기준년월**: {TARGET_MONTH}
- **대상**: 생명보험(H), 손해보험(I)
""")

# 실행 버튼
if st.button("데이터 수집 시작 (Start)", type="primary"):
    if not API_KEY:
        st.error("API Key를 입력해주세요.")
    else:
        # 비동기 함수 실행
        raw_data = asyncio.run(run_async_collection())

        if raw_data:
            df = pd.DataFrame(raw_data)
            
            # 전처리
            df['값'] = pd.to_numeric(df['값'].astype(str).str.replace(',', ''), errors='coerce')

            # 피벗 테이블
            df_pivot = df.pivot_table(
                index=['구분', '회사명', '기준년월'],
                columns='계정명',
                values='값',
                aggfunc='first'
            ).reset_index()

            # 결과 탭 구성
            tab1, tab2 = st.tabs(["📋 요약 테이블 (Pivot)", "raw 원본 데이터"])

            with tab1:
                st.subheader("결과 데이터")
                st.dataframe(df_pivot, use_container_width=True)
                
                # CSV 다운로드
                csv = df_pivot.to_csv(index=False, encoding='utf-8-sig')
                st.download_button(
                    label="💾 결과 다운로드 (CSV)",
                    data=csv,
                    file_name=f"insurance_solvency_{TARGET_MONTH}_pivot.csv",
                    mime="text/csv"
                )

            with tab2:
                st.dataframe(df, use_container_width=True)
        else:
            st.warning("수집된 데이터가 없습니다. API Key나 기준년월을 확인해주세요.")