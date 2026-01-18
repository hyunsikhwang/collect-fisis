import streamlit as st
import aiohttp
import asyncio
import pandas as pd
import nest_asyncio
import json
import time

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
    status_container = st.status("🚀 데이터 수집을 준비합니다...", expanded=True)
    
    try:
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

            # 2. 작업 생성
            tasks = []
            semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
            
            status_container.write("📦 2. 통계 데이터 요청 생성 중...")
            
            for comp in life_companies:
                for acc in life_accounts:
                    tasks.append(fetch_statistics(session, semaphore, comp, acc, None, None))
            for comp in non_life_companies:
                for acc in non_life_accounts:
                    tasks.append(fetch_statistics(session, semaphore, comp, acc, None, None))

            total_tasks = len(tasks)
            status_container.write(f"📡 총 {total_tasks} 건의 API 호출을 시작합니다...")

            # 3. 실행 및 진행률 표시
            results = []
            progress_bar = status_container.progress(0)
            
            # as_completed를 사용하여 완료되는대로 진행률 업데이트
            completed_count = 0
            
            # 청크 단위로 나누어 UI 업데이트 부하 줄이기 (선택 사항이나 여기서는 실시간성 유지)
            for f in asyncio.as_completed(tasks):
                res = await f
                if res:
                    results.append(res)
                
                completed_count += 1
                # 진행률 업데이트 (0.0 ~ 1.0)
                if total_tasks > 0:
                    progress_bar.progress(completed_count / total_tasks)

            status_container.update(label="✅ 데이터 수집 완료!", state="complete", expanded=False)
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