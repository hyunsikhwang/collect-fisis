import streamlit as st
import aiohttp
import asyncio
import pandas as pd
import nest_asyncio
import json
import time
import duckdb
import os
import plotly.graph_objects as go
import plotly.express as px
import requests
from datetime import datetime

# Streamlit 페이지 설정
st.set_page_config(
    page_title="보험사 지급여력비율 수집기",
    page_icon="📊",
    layout="wide"
)

# 비동기 루프 충돌 방지
nest_asyncio.apply()

# ==========================================
# 1. 상수 및 기본 설정
# ==========================================
# API 키 (st.secrets 처리 후 필요시 UI에서 입력)
API_KEY = st.secrets.get("FSS_API_KEY", "")
TARGET_MONTH = "202509" # 기본값 설정

TERM = "Q" # 분기
BASE_URL = "http://fisis.fss.or.kr/openapi"
MAX_CONCURRENT_REQUESTS = 20

# ==========================================
# 1.5. MotherDuck DB 설정
# ==========================================
MD_TOKEN = st.secrets.get("MOTHERDUCK_TOKEN", "")
DB_NAME = "fisis_cache"
TABLE_NAME = "insurance_stats"
COLUMNS = ['구분', '회사코드', '회사명', '계정코드', '계정명', '기준년월', '단위', '값']

def get_md_connection():
    """MotherDuck 연결 설정"""
    if not MD_TOKEN:
        return None
    try:
        # MotherDuck 연결 (md: 뒤에 토큰이 없으면 st.secrets에서 가져오거나 환경변수 확인)
        conn = duckdb.connect(f"md:?motherduck_token={MD_TOKEN}")
        # 데이터베이스 생성 및 사용
        conn.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}")
        conn.execute(f"USE {DB_NAME}")
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
            # 컬럼 순서 고정 및 데이터 클리닝
            df_to_save = df[COLUMNS].copy()
            for col in ['회사코드', '계정코드', '기준년월']:
                df_to_save[col] = df_to_save[col].astype(str).str.strip()

            # 임시 뷰를 생성하여 데이터를 적재
            conn.register("df_to_save", df_to_save)
            # 명시적으로 컬럼을 지정하여 INSERT (순서 일관성 보장)
            col_names = ", ".join(COLUMNS) + ", 수집일시"
            conn.execute(f"INSERT INTO {TABLE_NAME} ({col_names}) SELECT *, CURRENT_TIMESTAMP FROM df_to_save")
            conn.close()
        except Exception as e:
            st.error(f"데이터 저장 실패: {e}")

def load_kics_analysis_data():
    """K-ICS 분석을 위한 전체 데이터 로드 및 계산"""
    conn = get_md_connection()
    if not conn:
        return pd.DataFrame()
    
    try:
        # 관심 있는 계정들만 필터링해서 가져오기
        target_accounts = [
            '지급여력금액(경과조치 적용 전)', 
            '지급여력기준금액(경과조치 적용 전)',
            '지급여력금액(경과조치 적용 후)', 
            '지급여력기준금액(경과조치 적용 후)'
        ]
        
        # [DEBUG] 디버깅 옵션 (Dashboard 상단에 표시됨)
        show_debug = st.checkbox("🔍 상세 데이터 추출 과정 확인 (디버거)", value=False)
        
        # 1. DB에 있는 모든 독특한 계정명 확인
        all_accounts = conn.execute(f"SELECT DISTINCT 계정명 FROM {TABLE_NAME}").df()['계정명'].tolist()
        if show_debug:
            st.write(f"DEBUG: DB 내 총 계정 수: {len(all_accounts)}")
            st.write(f"DEBUG: DB 내 계정 샘플: {all_accounts[:5]}")
        
        # 2. 유사한 계정명 매핑 (공백 제거 및 부분 일치 검색으로 강화)
        def find_best_match(target, candidates):
            target_clean = target.replace(" ", "")
            # 완전 일치(공백 제거)
            for c in candidates:
                if c.replace(" ", "") == target_clean:
                    return c
            # 부분 일치 검색
            for c in candidates:
                if target_clean in c.replace(" ", "") or c.replace(" ", "") in target_clean:
                    return c
            return target

        actual_targets = [find_best_match(t, all_accounts) for t in target_accounts]
        if show_debug:
            st.write(f"DEBUG: 매핑된 타겟 계정: {actual_targets}")
        
        # IN 절 파라미터 생성
        placeholders = ', '.join(['?' for _ in actual_targets])
        query = f"SELECT * FROM {TABLE_NAME} WHERE 계정명 IN ({placeholders})"
        df = conn.execute(query, actual_targets).df()
        conn.close()
        
        if show_debug:
            st.write(f"DEBUG: 조회된 로우 수: {len(df)}")

        if df.empty:
            return pd.DataFrame()

        # 데이터 클리닝
        df['기준년월'] = df['기준년월'].astype(str).str.strip()
        
        # 매핑용 사전 생성 (원래 이름으로 통일)
        name_map = dict(zip(actual_targets, target_accounts))
        df['계정명'] = df['계정명'].map(name_map)
        
        if show_debug:
            st.write("DEBUG: 계정명 매핑 후 데이터 샘플:", df.head())

        # 피벗하여 계산하기 쉽게 변환
        # 계정명이 중복될 수 있으므로 (동일 회사가 같은 달에 여러번 수집된 경우 등) sum으로 집계
        pdf = df.pivot_table(
            index=['구분', '기준년월', '회사명'],
            columns='계정명',
            values='값',
            aggfunc='sum'
        ).reset_index()
        
        if show_debug:
            st.write("DEBUG: 피벗 후 데이터 컬럼:", pdf.columns.tolist())
            st.write("DEBUG: 피벗 후 데이터 수:", len(pdf))
        pdf = df.pivot_table(
            index=['구분', '기준년월', '회사명'],
            columns='계정명',
            values='값',
            aggfunc='sum'
        ).reset_index()
        
        # 필요한 컬럼이 있는지 확인 (없으면 0으로 채움)
        for col in target_accounts:
            if col not in pdf.columns:
                pdf[col] = 0

        # 그룹별 합계 계산 (생명보험, 손해보험, 전체)
        # 1. 생명/손해별 합계
        grouped = pdf.groupby(['구분', '기준년월'])[target_accounts].sum().reset_index()
        
        # 2. 전체(Total) 합계 생성
        total = pdf.groupby(['기준년월'])[target_accounts].sum().reset_index()
        total['구분'] = '전체'
        
        # 결합
        final_df = pd.concat([grouped, total], ignore_index=True)
        
        # K-ICS 비율 계산 (%)
        # 경과조치 전
        final_df['ratio_before'] = (final_df['지급여력금액(경과조치 적용 전)'] / 
                                    final_df['지급여력기준금액(경과조치 적용 전)'].replace(0, pd.NA)) * 100
        # 경과조치 후
        final_df['ratio_after'] = (final_df['지급여력금액(경과조치 적용 후)'] / 
                                   final_df['지급여력기준금액(경과조치 적용 후)'].replace(0, pd.NA)) * 100
        
        # 정렬 (날짜순)
        final_df = final_df.sort_values('기준년월')
        
        return final_df
    except Exception as e:
        st.error(f"분석 데이터 로드 실패: {e}")
        return pd.DataFrame()

def fetch_ecos_bond_yield(start_month, end_month):
    """ECOS에서 국고채 10년 금리 조회"""
    ECOS_API_KEY = st.secrets.get("ECOS_API_KEY", "")
    if not ECOS_API_KEY:
        return pd.DataFrame()
    
    # K-ICS 데이터 범위에 맞춰 시작/종료일 설정
    # start_month/end_month: '202303' 형식 -> '20230301' / '20230331' 등으로 변환 필요하나
    # ECOS는 단순히 앞뒤 날짜만 넉넉히 주면 됨
    start_date = f"{start_month}01"
    # 현재 날짜 기준
    KST = timezone('Asia/Seoul')
    nowSeo = datetime.now(KST).strftime('%Y%m%d')
    
    bond_cd = '010210000' # 국고채 10년
    url = f'http://ecos.bok.or.kr/api/StatisticSearch/{ECOS_API_KEY}/json/kr/1/10000/817Y002/D/{start_date}/{nowSeo}/{bond_cd}'

    try:
        res = requests.get(url, timeout=10)
        data = res.json()
        if 'StatisticSearch' in data and 'row' in data['StatisticSearch']:
            rows = data['StatisticSearch']['row']
            df = pd.DataFrame(rows)
            df['yield'] = df['DATA_VALUE'].astype(float)
            # TIME: 20230301 -> 기준년월 202303 추출
            df['기준년월'] = df['TIME'].str[:6]
            
            # 월별 마지막 영업일 기준 금리 추출 (K-ICS 대비용)
            df_monthly = df.groupby('기준년월').last().reset_index()[['기준년월', 'yield']]
            return df_monthly
    except Exception as e:
        st.warning(f"ECOS 금리 데이터 로드 실패: {e}")
    return pd.DataFrame()

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
                '기준년월': TARGET_MONTH, # API 결과와 상관없이 요청한 기준년월로 저장 (일관성 유지)
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
        else:
            status_container.write("ℹ️ 해당 월의 캐시된 데이터가 없습니다.")
        
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
                # 데이터 타입을 문자열로 강제 변환 및 공백 제거 (캐시 미스 방지)
                existing_keys = set(zip(
                    cached_df['회사코드'].astype(str).str.strip(), 
                    cached_df['계정코드'].astype(str).str.strip()
                ))

            def build_tasks(companies, accounts):
                for comp in companies:
                    for acc in accounts:
                        # 비교 시에도 문자열로 변환 및 공백 제거
                        f_cd = str(comp['financeCd']).strip()
                        a_cd = str(acc['accountCd']).strip()
                        if (f_cd, a_cd) not in existing_keys:
                            tasks.append(fetch_statistics(session, semaphore, comp, acc, None, None))

            build_tasks(life_companies, life_accounts)
            build_tasks(non_life_companies, non_life_accounts)

            total_tasks = len(tasks)
            
            if total_tasks == 0:
                status_container.write("✨ 모든 데이터가 이미 캐시되어 있습니다.")
                status_container.update(label="✅ 캐시 데이터 리로드 완료!", state="complete", expanded=False)
                return cached_df.to_dict('records')

            status_container.write(f"📡 {len(existing_keys)}건은 캐시에서 발견했고, {total_tasks} 건의 새로운 데이터를 API로 수집합니다...")

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
                    # 컬럼 순서 및 이름 일관성 확보
                    all_results_df = pd.concat([cached_df[COLUMNS], new_df[COLUMNS]], ignore_index=True)
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
st.title("📊 보험사 지급여력비율 분석 대시보드")

# 메인 탭 분리: 분석 대시보드와 데이터 수집기
main_tab1, main_tab2 = st.tabs(["📈 분석 대시보드 (Dashboard)", "📡 데이터 수집기 (Collector)"])

with main_tab1:
    st.subheader("📊 K-ICS 비율 추이 분석")
    st.info("MotherDuck에 저장된 모든 과거 데이터를 기반으로 시계열 분석을 수행합니다.")
    
    analysis_df = load_kics_analysis_data()
    
    if not analysis_df.empty:
        # Plotly 차트 생성
        fig = go.Figure()
        
        # 색상 및 스타일 설정
        styles = {
            '생명보험': {'color': '#1f77b4'},
            '손해보험': {'color': '#ff7f0e'},
            '전체': {'color': '#2ca02c'}
        }
        
        for g in ['생명보험', '손해보험', '전체']:
            g_df = analysis_df[analysis_df['구분'] == g]
            
            # 경과조치 적용 전 (점선)
            fig.add_trace(go.Scatter(
                x=g_df['기준년월'], 
                y=g_df['ratio_before'],
                name=f"{g} (경과조치 전)",
                line=dict(color=styles[g]['color'], dash='dot', width=2),
                mode='markers+lines',
                marker=dict(size=8)
            ))
            
            # 경과조치 적용 후 (실선)
            fig.add_trace(go.Scatter(
                x=g_df['기준년월'], 
                y=g_df['ratio_after'],
                name=f"{g} (경과조치 후)",
                line=dict(color=styles[g]['color'], width=4),
                mode='markers+lines',
                marker=dict(size=10)
            ))
        
        fig.update_layout(
            title="보험업권별 K-ICS 비율 및 국고채 10년 금리 추이",
            xaxis_title="기준년월",
            yaxis_title="K-ICS Ratio (%)",
            yaxis2=dict(
                title="국고채 10년 금리 (%)",
                overlaying='y',
                side='right',
                showgrid=False
            ),
            legend_title="구분",
            template="plotly_white",
            hovermode="x unified",
            height=600,
            yaxis=dict(ticksuffix="%")
        )

        # ECOS 금리 데이터 추가
        min_month = analysis_df['기준년월'].min()
        max_month = analysis_df['기준년월'].max()
        
        bond_df = fetch_ecos_bond_yield(min_month, max_month)
        
        if not bond_df.empty:
            # 시각화 기간에 맞게 필터링
            bond_df = bond_df[(bond_df['기준년월'] >= min_month) & (bond_df['기준년월'] <= max_month)]
            
            fig.add_trace(go.Scatter(
                x=bond_df['기준년월'],
                y=bond_df['yield'],
                name="국고채 10년 (우축)",
                line=dict(color='gray', width=3, dash='dash'),
                yaxis='y2',
                mode='lines+markers',
                marker=dict(symbol='diamond', size=10)
            ))
        else:
            if not st.secrets.get("ECOS_API_KEY"):
                st.caption("ℹ️ ECOS_API_KEY를 설정하면 국고채 금리를 함께 보실 수 있습니다.")
        
        st.plotly_chart(fig, use_container_width=True)
        
        # 분석 데이터 테이블
        with st.expander("📍 상세 수치 데이터 확인"):
            st.dataframe(analysis_df, use_container_width=True)
    else:
        st.warning("표시할 분석 데이터가 없습니다. 먼저 '데이터 수집기' 탭에서 데이터를 수집해 주세요.")
        
        # 디버깅을 위한 데이터 현황 세션 (Dashboard에서도 데이터가 없을 때 표시)
        with st.expander("🛠️ 데이터베이스 현황 확인 (디버깅)"):
            conn = get_md_connection()
            if conn:
                try:
                    count = conn.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
                    st.write(f"현재 총 레코드 수: {count}건")
                    
                    st.write("보관 중인 계정명 목록:")
                    distinct_accounts = conn.execute(f"SELECT DISTINCT 계정명 FROM {TABLE_NAME}").df()
                    st.dataframe(distinct_accounts)
                    
                    st.write("보관 중인 기준년월 목록:")
                    distinct_months = conn.execute(f"SELECT DISTINCT 기준년월 FROM {TABLE_NAME} ORDER BY 기준년월").df()
                    st.dataframe(distinct_months)
                    
                    conn.close()
                except Exception as e:
                    st.error(f"현황 확인 중 오류: {e}")
            else:
                st.warning("MotherDuck 연결 실패 (토큰 확인 필요)")

with main_tab2:
    st.subheader("📡 FSS Open API 데이터 수집")
    
    # 설정 섹션 (기존 사이드바에서 이동)
    with st.expander("⚙️ 수집 설정 (Settings)", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            if not st.secrets.get("FSS_API_KEY"):
                API_KEY = st.text_input(
                    "금융감독원 API Key", 
                    value=API_KEY,
                    type="password",
                    help="인증키를 입력하세요."
                )
            else:
                st.success("✅ API Key가 로드되었습니다.")
                API_KEY = st.secrets.get("FSS_API_KEY")
        
        with col2:
            TARGET_MONTH = st.text_input(
                "수집 기준년월 (YYYYMM)", 
                value="202509",
                help="조회하고 싶은 년월을 입력하세요."
            )

    st.markdown(f"""
    Open API를 사용하여 보험사의 지급여력비율 관련 데이터를 수집하고 MotherDuck에 저장합니다.
    - **대상**: 생명보험(H), 손해보험(I)
    """)
    
    # 실행 버튼
    if st.button("🚀 데이터 수집 시작 (Start Collection)", type="primary"):
        if not API_KEY:
            st.error("API Key를 입력해주세요. (사이드바에서 입력 가능)")
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

                # 결과 섹션
                st.divider()
                st.success(f"✅ {TARGET_MONTH} 데이터 처리가 완료되었습니다.")
                
                tab_res1, tab_res2 = st.tabs(["📋 요약 테이블 (Pivot)", "📄 RAW 데이터"])
                
                with tab_res1:
                    st.subheader(f"{TARGET_MONTH} 수집 결과 (요약)")
                    st.dataframe(df_pivot, use_container_width=True)
                    
                    # CSV 다운로드
                    csv = df_pivot.to_csv(index=False, encoding='utf-8-sig')
                    st.download_button(
                        label="💾 수집 결과 다운로드 (CSV)",
                        data=csv,
                        file_name=f"insurance_solvency_{TARGET_MONTH}_result.csv",
                        mime="text/csv"
                    )

                with tab_res2:
                    st.subheader(f"{TARGET_MONTH} RAW 데이터")
                    st.dataframe(df, use_container_width=True)
                
                # 수집이 완료되었으니 화면 갱신을 유도하거나 정보를 제공
                st.info("💡 새로운 데이터가 저장되었습니다. '분석 대시보드' 탭으로 이동하여 차트를 확인해 보세요.")
            else:
                st.warning("수집된 데이터가 없습니다. API Key나 기준년월을 확인해주세요.")