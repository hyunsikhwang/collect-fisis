import streamlit as st
import aiohttp
import asyncio
import pandas as pd
import nest_asyncio
import json
import time
import duckdb
import os
import requests
from datetime import datetime
from pytz import timezone
from streamlit_echarts import st_pyecharts
from pyecharts import options as opts
from pyecharts.charts import Line, Bar
from pyecharts.commons.utils import JsCode

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

# 회사명 한/영 매핑 (표시용)
CompKoEn = {
    'DB생명보험주식회사': 'DB Life',
    'DGB생명보험주식회사': 'iM(DGB) Life',
    '아이엠라이프생명보험 주식회사': 'iM(DGB) Life',
    'KB라이프생명보험': 'KB Life',
    '교보라이프플래닛생명보험주식회사': 'KyoboLP Life',
    '교보생명보험주식회사': 'Kyobo Life',
    '농협생명보험주식회사': 'NH Life',
    '미래에셋생명보험주식회사': 'MiraeAsset Life',
    '삼성생명보험주식회사': 'Samsung Life',
    '신한라이프생명보험주식회사': 'Shinhan Life',
    '아이비케이연금보험 주식회사': 'IBK Life',
    '케이디비생명보험주식회사': 'KDB Life',
    '하나생명보험주식회사': 'Hana Life',
    '한화생명보험주식회사': 'Hanwha Life',
    '흥국생명보험주식회사': 'HeungKuk Life',
    '동양생명보험주식회사': 'Tongyang Life',
    '라이나생명보험주식회사': 'Lina Life',
    '메트라이프생명보험(주)': 'Met Life',
    '비엔피파리바카디프생명보험주식회사': 'Cardif Life',
    '에이비엘생명보험주식회사': 'ABL Life',
    '에이아이에이생명보험 주식회사': 'AIA Life',
    '처브라이프생명보험주식회사': 'Chubb Life',
    '푸본현대생명보험주식회사': 'Fubon Life',
    'DB손해보험주식회사': 'DB FM',
    '농협손해보험주식회사': 'NH FM',
    '롯데손해보험주식회사': 'Lotte FM',
    '메리츠화재해상보험주식회사': 'Meritz FM',
    '삼성화재해상보험주식회사': 'Samsung FM',
    '엠지손해보험주식회사': 'MG FM',
    '주식회사KB손해보험': 'KB FM',
    '하나손해보험주식회사': 'Hana FM',
    '한화손해보험주식회사': 'Hanwha FM',
    '현대해상화재보험주식회사': 'Hyundai FM',
    '흥국화재해상보험주식회사': 'Heungkuk FM',
    '신한EZ손해보험주식회사': 'ShinhanEZ FM',
    '주식회사 카카오페이손해보험': 'Kakao FM',
    '캐롯손해보험주식회사': 'Carrot FM',
    '악사손해보험주식회사': 'AXA FM',
    '에이스아메리칸화재해상보험주식회사': 'Ace FM',
    '에이아이지손해보험주식회사': 'AIG FM',
    '뮌헨재보험주식회사 한국지점': 'Munich Re',
    '스위스리 아시아 피티이 엘티디 한국지점': 'Swiss Re',
    '스코리인슈어런스아시아퍼시픽피티이엘티디한국지점': 'Scor Re',
    '알지에이 리인슈어런스 컴파니 한국지점': 'RGA',
    '제너럴재보험주식회사 서울지점': 'Gen Re',
    '코리안리재보험주식회사': 'Korean Re',
    '퍼시픽라이프리 인터내셔널 한국지점': 'Pacific Re',
    '하노버재보험(주) 한국지점': 'Hanover Re'
}

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

def shorten_company_name(name):
    """회사명을 보기 좋게 축약 (주식회사, (주), 한국지점 등 제거)"""
    if not name:
        return ""
    
    # 제거할 단어 목록
    removals = [
        "주식회사", "(주)", "주", 
        "생명보험", "손해보험", "화재해상보험", "화재보험", "해상보험",
        "한국지점"
    ]
    
    short_name = name
    for r in removals:
        short_name = short_name.replace(r, "")
    
    return short_name.strip()

def get_english_company_name(name):
    """차트 표시용 영문 회사명 반환"""
    if not name:
        return ""
    return CompKoEn.get(name, "")

def render_sector_chart(sector, filtered_df, company_df, color_sets, weighted_avg):
    """특정 업권의 누적 바 차트 및 평균선을 렌더링"""
    import pandas as pd
    from pyecharts import options as opts
    from pyecharts.charts import Bar
    from pyecharts.commons.utils import JsCode
    from streamlit_echarts import st_pyecharts

    st.write(f"### {sector}")
    
    # 해당 업권 데이터 필터링 및 정렬
    s_df = filtered_df[filtered_df['구분'] == sector].sort_values('final_ratio', ascending=False)
    
    if s_df.empty:
        st.info(f"{sector} 데이터가 없습니다.")
        return

    # 누적 차트를 위한 데이터 준비
    base_ratios = [] # 하단 (A)
    effect_ratios = [] # 상단 (D-A)
    total_ratios = [] # 레이블 표시용 (D)
    
    for _, row in s_df.iterrows():
        a_val = float(row['A'])
        d_val = float(row['final_ratio'])
        
        if row['is_fallback']:
            base_ratios.append(int(round(a_val, 0)))
            effect_ratios.append(0)
        else:
            base_ratios.append(int(round(a_val, 0)))
            effect_ratios.append(max(0, int(round(d_val - a_val, 0))))
        
        total_ratios.append(int(round(d_val, 0)))

    bar = Bar(init_opts=opts.InitOpts(width="100%", height="500px", theme="white", renderer="svg"))
    bar.add_xaxis(xaxis_data=s_df['short_display_name'].tolist())
    
    # 1. 하단 바: 경과조치 전
    bar.add_yaxis(
        series_name="경과조치 전",
        y_axis=base_ratios,
        stack="stack1",
        label_opts=opts.LabelOpts(is_show=False),
        itemstyle_opts=opts.ItemStyleOpts(color=color_sets[sector][0])
    )
    
    # 2. 상단 바: 경과조치 효과
    bar.add_yaxis(
        series_name="경과조치 효과",
        y_axis=effect_ratios,
        stack="stack1",
        label_opts=opts.LabelOpts(
            is_show=True, 
            position="top", 
            formatter=JsCode("""function(params) {
                var total_ratios = """ + str(total_ratios) + """;
                return total_ratios[params.dataIndex] + '%';
            }""")
        ),
        itemstyle_opts=opts.ItemStyleOpts(color=color_sets[sector][1]),
        markline_opts=opts.MarkLineOpts(
            data=[{"yAxis": round(weighted_avg, 2), "name": f"업권 평균 ({round(weighted_avg, 1)}%)"}],
            label_opts=opts.LabelOpts(formatter=f"{sector} 평균: {round(weighted_avg, 1)}%", position="insideEndTop"),
            linestyle_opts=opts.LineStyleOpts(type_="dashed", width=1, color="#D10000")
        )
    )
    
    bar.set_global_opts(
        title_opts=opts.TitleOpts(title=f"{sector}사별 K-ICS 비율"),
        xaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(rotate=45, interval=0, font_size=11)),
        yaxis_opts=opts.AxisOpts(name="비율 (%)", axislabel_opts=opts.LabelOpts(formatter="{value}%")),
        tooltip_opts=opts.TooltipOpts(
            trigger="axis", 
            axis_pointer_type="shadow",
            formatter=JsCode("""function(params) {
                var res = params[0].name + '<br/>';
                var total = 0;
                for(var i=0; i<params.length; i++) {
                    res += params[i].marker + params[i].seriesName + ': ' + params[i].value + '%<br/>';
                    total += params[i].value;
                }
                res += '<b>최종 비율 (경과후): ' + total + '%</b>';
                return res;
            }""")
        ),
    )
    
    st_pyecharts(bar, height="500px", key=f"bar_{sector}", renderer="svg")

def get_available_months():
    """DB에 저장된 모든 기준년월 목록을 내림차순으로 반환"""
    conn = get_md_connection()
    if not conn:
        return []
    try:
        # 최신순 정렬
        df = conn.execute(f"SELECT DISTINCT 기준년월 FROM {TABLE_NAME} ORDER BY 기준년월 DESC").df()
        conn.close()
        return df['기준년월'].tolist()
    except Exception as e:
        st.error(f"기준년월 목록 조회 실패: {e}")
        return []

def load_company_solvency_data(target_month):
    """보험사별 특정 기준년월 지급여력비율 데이터 로드 및 전처리"""
    if not target_month:
        return pd.DataFrame(), ""

    conn = get_md_connection()
    if not conn:
        return pd.DataFrame(), target_month
    
    try:
        # 1. 필요한 계정코드 데이터 가져오기
        # A, B, C, D, E, F (E, F는 가중평균 계산용 분자/분모, B, C는 Fallback용)
        query = f"""
            SELECT 구분, 회사명, 계정코드, 값, 기준년월
            FROM {TABLE_NAME}
            WHERE 기준년월 = ? AND 계정코드 IN ('A', 'B', 'C', 'D', 'E', 'F')
        """
        df = conn.execute(query, [target_month]).df()
        conn.close()

        if df.empty:
            return pd.DataFrame(), target_month

        # 3. 피벗하여 A, D 컬럼으로 분리
        pdf = df.pivot_table(
            index=['구분', '회사명', '기준년월'],
            columns='계정코드',
            values='값',
            aggfunc='first'
        ).reset_index()

        # 컬럼 존재 확인 및 0 채우기
        for c_code in ['A', 'B', 'C', 'D', 'E', 'F']:
            if c_code not in pdf.columns:
                pdf[c_code] = 0
            else:
                pdf[c_code] = pdf[c_code].fillna(0)

        # 4. 개별 회사 Fallback 로직 및 유효 금액 계산
        def process_row(row):
            # A, D 기반 최종 비율 (단순 표시용)
            # D가 유효하고 A와 다른 경우에만 '경과후' 사용
            if row['D'] > 0 and row['D'] != row['A']:
                final_r = row['D']
                is_fb = False
            else:
                final_r = row['A']
                is_fb = True
            
            # 가중 평균용 유효 금액 계산
            # 분자(지급여력금액): E > 0 ? E : B
            eff_num = row['E'] if row['E'] > 0 else row['B']
            # 분모(기준금액): F > 0 ? F : C
            eff_den = row['F'] if row['F'] > 0 else row['C']
            
            return pd.Series([final_r, is_fb, eff_num, eff_den])

        pdf[['final_ratio', 'is_fallback', 'eff_num', 'eff_den']] = pdf.apply(process_row, axis=1)

        # 5. 표시용 회사명 처리
        pdf['display_name'] = pdf.apply(
            lambda r: f"{shorten_company_name(r['회사명'])}*" if not r['is_fallback'] else shorten_company_name(r['회사명']),
            axis=1
        )

        # Downstream logic can use stable ASCII aliases regardless of locale/encoding.
        dim_cols = pdf.columns[:3].tolist()
        if len(dim_cols) >= 3:
            pdf['sector'] = pdf[dim_cols[0]]
            pdf['company_name'] = pdf[dim_cols[1]]
            pdf['base_month'] = pdf[dim_cols[2]]

        return pdf, target_month
    except Exception as e:
        st.error(f"회사별 데이터 로드 실패: {e}")
        return pd.DataFrame(), ""

def build_company_change_df(current_df, previous_df):
    """Create latest-vs-previous K-ICS deltas per company."""
    if current_df.empty or previous_df.empty:
        return pd.DataFrame()

    curr = current_df.copy()
    prev = previous_df.copy()

    for df in [curr, prev]:
        if 'sector' not in df.columns or 'company_name' not in df.columns:
            continue
        if 'A' not in df.columns:
            df['A'] = 0
        if 'D' not in df.columns:
            df['D'] = 0
        df['ratio_before'] = pd.to_numeric(df['A'], errors='coerce').fillna(0)
        # Keep "after" consistent with existing fallback rule when D is missing/invalid.
        df['ratio_after'] = df.apply(lambda r: r['D'] if pd.notnull(r['D']) and r['D'] > 0 else r['A'], axis=1)

    merged = curr[['sector', 'company_name', 'ratio_before', 'ratio_after']].merge(
        prev[['sector', 'company_name', 'ratio_before', 'ratio_after']],
        on=['sector', 'company_name'],
        suffixes=('_current', '_previous'),
        how='inner'
    )

    if merged.empty:
        return pd.DataFrame()

    merged['delta_before'] = merged['ratio_before_current'] - merged['ratio_before_previous']
    merged['delta_after'] = merged['ratio_after_current'] - merged['ratio_after_previous']
    return merged

def render_company_change_chart(change_df, sector, delta_col, chart_title, key_suffix):
    """Render a diverging horizontal bar chart for company-level deltas."""
    s_df = change_df[change_df['sector'] == sector].copy()
    if s_df.empty:
        st.info(f"No data in {sector} sector.")
        return

    s_df = s_df.sort_values(delta_col, ascending=False)
    if "english_name" in s_df.columns:
        s_df['display_name'] = s_df['english_name']
    else:
        s_df['display_name'] = s_df['company_name'].map(get_english_company_name).fillna("")
    s_df = s_df[s_df['display_name'].astype(str).str.strip() != ""].copy()
    if s_df.empty:
        st.info(f"No companies with English names in {sector} sector.")
        return

    x_names = s_df['display_name'].tolist()
    y_delta = [round(float(v), 1) for v in s_df[delta_col]]
    prev_col = "ratio_before_previous" if delta_col == "delta_before" else "ratio_after_previous"
    curr_col = "ratio_before_current" if delta_col == "delta_before" else "ratio_after_current"
    y_prev = [round(float(v), 2) for v in s_df[prev_col]]
    y_curr = [round(float(v), 2) for v in s_df[curr_col]]

    # Axis-break-like compression for extreme outliers so smaller changes stay visible.
    abs_delta = sorted([abs(v) for v in y_delta], reverse=True)
    apply_axis_break = False
    break_start = 0.0
    break_scale = 0.35
    if len(abs_delta) >= 2 and abs_delta[1] > 0:
        ratio_gap = abs_delta[0] / abs_delta[1]
        if ratio_gap >= 2.8 and abs_delta[0] >= 20:
            apply_axis_break = True
            break_start = round(max(8.0, abs_delta[1] * 1.15), 1)

    def compress_delta(v):
        if not apply_axis_break:
            return round(v, 3)
        sign = -1 if v < 0 else 1
        av = abs(v)
        if av <= break_start:
            return round(v, 3)
        return round(sign * (break_start + (av - break_start) * break_scale), 3)

    chart_points = [{"value": compress_delta(v), "actual": v} for v in y_delta]
    max_abs_for_axis = max(abs(compress_delta(v)) for v in y_delta) if y_delta else 1
    axis_pad = max(2.0, max_abs_for_axis * 0.08)
    axis_min = round(-(max_abs_for_axis + axis_pad), 2)
    axis_max = round(max_abs_for_axis + axis_pad, 2)

    bar = Bar(init_opts=opts.InitOpts(width="100%", height="520px", theme="white", renderer="svg"))
    bar.add_xaxis(xaxis_data=x_names)
    bar.add_yaxis(
        series_name="Delta (latest-previous, %p)",
        y_axis=chart_points,
        label_opts=opts.LabelOpts(
            is_show=True,
            position="right",
            formatter=JsCode(
                "function(p){"
                "var raw=(p.data && p.data.actual!==undefined)?p.data.actual:p.value;"
                "return (raw > 0 ? '+' : '') + Number(raw).toFixed(1) + '%p';"
                "}"
            )
        ),
        itemstyle_opts=opts.ItemStyleOpts(
            color=JsCode(
                """
            function(params){
                var raw=(params.data && params.data.actual!==undefined)?params.data.actual:params.value;
                if(raw > 0){return '#1a9850';}
                if(raw < 0){return '#d73027';}
                return '#7f8c8d';
            }
            """
            )
        )
    )
    bar.reversal_axis()
    bar.set_global_opts(
        title_opts=opts.TitleOpts(title=chart_title, top=8),
        legend_opts=opts.LegendOpts(top=40),
        grid_opts=opts.GridOpts(left="34%", right="8%", top=96, bottom=28, contain_label=False),
        xaxis_opts=opts.AxisOpts(
            name="Delta (%p)",
            min_=axis_min,
            max_=axis_max,
            axislabel_opts=opts.LabelOpts(
                formatter=JsCode(
                    "function(v){"
                    f"var hasBreak={str(apply_axis_break).lower()};"
                    f"var b={break_start};"
                    f"var c={break_scale};"
                    "if(!hasBreak){return Number(v).toFixed(1);}"
                    "var s=v<0?-1:1; var a=Math.abs(v);"
                    "if(a<=b){return Number(v).toFixed(1);}"
                    "var restored=s*(b+((a-b)/c));"
                    "return Number(restored).toFixed(1);"
                    "}"
                )
            )
        ),
        yaxis_opts=opts.AxisOpts(axislabel_opts=opts.LabelOpts(font_size=11)),
        tooltip_opts=opts.TooltipOpts(
            trigger="item",
            formatter=JsCode(
                "function(p){"
                f"var prev={json.dumps(y_prev)}; var curr={json.dumps(y_curr)};"
                "var d=(p.data && p.data.actual!==undefined)?p.data.actual:p.value; var sign=d>0?'+':'';"
                "return p.name + '<br/>Previous: ' + prev[p.dataIndex] + '%<br/>Latest: ' + curr[p.dataIndex] + '%<br/><b>Delta: ' + sign + Number(d).toFixed(1) + '%p</b>';"
                "}"
            )
        ),
    )

    if apply_axis_break:
        bar.set_series_opts(
            markline_opts=opts.MarkLineOpts(
                data=[
                    opts.MarkLineItem(x=0),
                    opts.MarkLineItem(x=compress_delta(break_start)),
                    opts.MarkLineItem(x=compress_delta(-break_start)),
                ]
            )
        )
        st.caption(
            f"Axis compression applied: |delta| >= {break_start:.1f}%p is shown at {break_scale:.2f}x scale."
        )
    else:
        bar.set_series_opts(markline_opts=opts.MarkLineOpts(data=[opts.MarkLineItem(x=0)]))

    st_pyecharts(bar, height="520px", key=f"company_change_{key_suffix}_{sector}", renderer="svg")


def reclassify_company_sector(sector, company_name):
    """Classify non-life rows into non-life/reinsurance/excluded buckets."""
    if sector == '생명보험':
        return '생명보험'
    if sector == '손해보험':
        if company_name in REINSURANCE_COMPANIES:
            return '재보험'
        if company_name in EXCLUDE_NON_LIFE:
            return '제외'
        return '손해보험'
    return sector

def apply_sector_reclassification(df):
    """Apply consistent sector classification for company-level charts."""
    if df.empty or 'sector' not in df.columns or 'company_name' not in df.columns:
        return df

    out = df.copy()
    out['sector'] = out.apply(lambda r: reclassify_company_sector(r['sector'], r['company_name']), axis=1)
    out = out[out['sector'] != '제외'].copy()
    return out

# 분석용 업권 분류 설정 (손해 업권 세분화용)
EXCLUDE_NON_LIFE = [
    '팩토리뮤추얼인슈런스컴퍼니 한국지점',
    '퍼스트어메리칸권원보험(주)한국지점',
    '미쓰이스미토모해상화재보험(주)한국지점',
    '스타인터내셔널인슈어런스싱가포르한국지점',
    '동경해상일동화재보험(주)서울지점[폐]',
    '서울보증보험주식회사',
    '마이브라운반려동물전문보험',
    '알리안츠글로벌코퍼레이트앤스페셜티에스이 한국지점'
]

REINSURANCE_COMPANIES = [
    '알지에이 리인슈어런스 컴파니 한국지점',
    '코리안리재보험주식회사',
    '스위스리 아시아 피티이 엘티디 한국지점',
    '스코리인슈어런스아시아퍼시픽피티이엘티디한국지점',
    '뮌헨재보험주식회사 한국지점',
    '제너럴재보험주식회사 서울지점',
    '퍼시픽라이프리 인터내셔널 한국지점',
    '하노버재보험(주) 한국지점'
]

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

# 메인 탭 분리: 분석 대시보드, 회사별 현황, 데이터 수집기
selected_tab = st.segmented_control(
    "메뉴 선택",
    ["📈 분석 대시보드 (Dashboard)", "📊 회사별 현황 (Company Status)", "📉 회사별 변동 (Company Change)", "📡 데이터 수집기 (Collector)"],
    default="📈 분석 대시보드 (Dashboard)",
    label_visibility="collapsed"
)

if selected_tab == "📈 분석 대시보드 (Dashboard)":
    st.subheader("📊 K-ICS 비율 추이 분석")
    st.info("MotherDuck에 저장된 모든 과거 데이터를 기반으로 시계열 분석을 수행합니다.")
    
    analysis_df = load_kics_analysis_data()
    
    if not analysis_df.empty:
        # ECharts용 데이터 준비
        x_data = sorted(analysis_df['기준년월'].unique().tolist())
        
        # 금리 데이터 가져오기
        min_month = analysis_df['기준년월'].min()
        max_month = analysis_df['기준년월'].max()
        bond_df = fetch_ecos_bond_yield(min_month, max_month)
        
        # 금리 데이터 싱크 맞추기
        if not bond_df.empty:
            kics_months = analysis_df['기준년월'].unique()
            bond_df = bond_df[bond_df['기준년월'].isin(kics_months)].sort_values('기준년월')
        
        # pyecharts Line 객체 생성
        line = Line(init_opts=opts.InitOpts(width="100%", height="600px", theme="white", renderer="svg"))
        line.add_xaxis(xaxis_data=x_data)
        
        # 색상 매핑
        colors = {
            '생명보험': '#1f77b4',
            '손해보험': '#ff7f0e',
            '전체': '#2ca02c'
        }
        
        for g in ['생명보험', '손해보험', '전체']:
            # x축 순서에 맞춰 정렬 및 누락값 처리
            g_df = analysis_df[analysis_df['구분'] == g].set_index('기준년월').reindex(x_data).reset_index()
            
            # 경과조치 후 (실선)
            line.add_yaxis(
                series_name=f"{g} (경과조치 후)",
                y_axis=[round(float(v), 2) if pd.notnull(v) else None for v in g_df['ratio_after']],
                symbol="circle",
                symbol_size=10,
                linestyle_opts=opts.LineStyleOpts(width=4, color=colors[g]),
                itemstyle_opts=opts.ItemStyleOpts(color=colors[g]),
                label_opts=opts.LabelOpts(is_show=False),
                is_smooth=False,
            )
            
            # 경과조치 전 (점선, 초기 비활성화)
            line.add_yaxis(
                series_name=f"{g} (경과조치 전)",
                y_axis=[round(float(v), 2) if pd.notnull(v) else None for v in g_df['ratio_before']],
                symbol="circle",
                                symbol_size=8,
                linestyle_opts=opts.LineStyleOpts(width=2, type_="dashed", color=colors[g]),
                itemstyle_opts=opts.ItemStyleOpts(color=colors[g]),
                label_opts=opts.LabelOpts(is_show=False),
                is_smooth=False,
            )
            
        # 보조축 추가 (금리용)
        line.extend_axis(
            yaxis=opts.AxisOpts(
                name="금리 (%)",
                type_="value",
                position="right",
                is_scale=True, # 데이터 범위에 맞춰 유동적 조정
                axislabel_opts=opts.LabelOpts(formatter="{value}%"),
                splitline_opts=opts.SplitLineOpts(is_show=False),
            )
        )
        
        if not bond_df.empty:
            b_df = bond_df.set_index('기준년월').reindex(x_data).reset_index()
            line.add_yaxis(
                series_name="국고채 10년 (우축)",
                y_axis=[round(float(v), 3) if pd.notnull(v) else None for v in b_df['yield']],
                yaxis_index=1,
                symbol="diamond",
                symbol_size=12,
                linestyle_opts=opts.LineStyleOpts(width=2, type_="dashed", color="#6e7074"),
                itemstyle_opts=opts.ItemStyleOpts(color="#6e7074"),
                label_opts=opts.LabelOpts(is_show=False),
            )
            
        # 초기 비활성화할 시리즈 맵 생성
        selected_map = {f"{g} (경과조치 전)": False for g in ['생명보험', '손해보험', '전체']}
        
        line.set_global_opts(
            title_opts=opts.TitleOpts(title="보험업권별 K-ICS 비율 및 국고채 10년 금리 추이", subtitle="기준년월별 현황"),
            tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="cross"),
            xaxis_opts=opts.AxisOpts(name="기준년월", type_="category", boundary_gap=True),
            yaxis_opts=opts.AxisOpts(
                name="K-ICS 비율 (%)",
                is_scale=True, # 데이터 범위에 맞춰 유동적 조정
                axislabel_opts=opts.LabelOpts(formatter="{value}%"),
                splitline_opts=opts.SplitLineOpts(is_show=True),
            ),
            legend_opts=opts.LegendOpts(
                pos_right="5%", 
                pos_top="5%",
                orient="vertical", # 차트 내부 가독성을 위해 세로 배치
                selected_map=selected_map,
                background_color="rgba(255,255,255,0.7)", # 약간의 배경 투명도
                border_color="#ccc"
            ),
            datazoom_opts=[
                opts.DataZoomOpts(range_start=0, range_end=100), 
                opts.DataZoomOpts(type_="inside", range_start=0, range_end=100)
            ],
            toolbox_opts=opts.ToolboxOpts(is_show=True),
        )
        
        st_pyecharts(line, height="600px", key="dashboard_line_chart", renderer="svg")
        
        with st.expander("📍 상세 수치 데이터 확인"):
            st.dataframe(analysis_df, width="stretch")
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
                    st.dataframe(distinct_accounts, width="stretch")
                    
                    st.write("보관 중인 기준년월 목록:")
                    distinct_months = conn.execute(f"SELECT DISTINCT 기준년월 FROM {TABLE_NAME} ORDER BY 기준년월").df()
                    st.dataframe(distinct_months, width="stretch")
                    
                    conn.close()
                except Exception as e:
                    st.error(f"현황 확인 중 오류: {e}")
            else:
                st.warning("MotherDuck 연결 실패 (토큰 확인 필요)")

elif selected_tab == "📊 회사별 현황 (Company Status)":
    st.subheader("📊 회사별 지급여력비율 현황")
    
    # 가용한 모든 기준년월 가져오기
    available_months = get_available_months()
    
    if available_months:
        # 기준년월 선택 영역
        col_m, col_e = st.columns([1, 2])
        with col_m:
            selected_month = st.selectbox(
                "📅 기준년월 선택", 
                options=available_months, 
                index=0,
                help="조회할 기준년월을 선택하세요. 최신 데이터가 상단에 위치합니다."
            )
        
        company_df, latest_m = load_company_solvency_data(selected_month)
    
        if not company_df.empty:
            # 업권 재분류 로직 (손해보험 세분화)
            def reclassify_sector(row):
                if row['구분'] == '생명보험':
                    return '생명보험'
                elif row['구분'] == '손해보험':
                    if row['회사명'] in REINSURANCE_COMPANIES:
                        return '재보험'
                    elif row['회사명'] in EXCLUDE_NON_LIFE:
                        return '제외'
                    else:
                        return '손해보험'
                return row['구분']

            company_df['구분'] = company_df.apply(reclassify_sector, axis=1)
            # 제외 대상 제거
            company_df = company_df[company_df['구분'] != '제외'].copy()

            # 영문명 매핑 검증 (누락이 있으면 차트 렌더링 중단)
            missing_companies = sorted([c for c in company_df['회사명'].unique().tolist() if c not in CompKoEn])
            if missing_companies:
                st.error(
                    f"영문 회사명 매핑 누락: {len(missing_companies)}개 회사. "
                    "누락 방지를 위해 차트 렌더링을 중단했습니다. CompKoEn에 아래 회사를 추가해 주세요."
                )
                st.dataframe(pd.DataFrame({'누락 회사명': missing_companies}), width="stretch")
                st.stop()

            st.markdown(f"**조회 시점: {latest_m}** ( * 표시: 경과조치 적용 후 비율 사용 )")
            
            # 제외할 회사 선택 UI
            all_companies = sorted(company_df['회사명'].unique().tolist())
            excluded_companies = st.multiselect(
                "📊 비교 분석에서 제외할 회사 선택 (선택 시 차트에서 제거됩니다)",
                options=all_companies,
                default=[],
                help="데이터값이 비정상적으로 크거나 작아 차트의 전체 형태를 왜곡하는 회사를 제외할 수 있습니다."
            )
            
            # 필터링 적용
            filtered_df = company_df[~company_df['회사명'].isin(excluded_companies)].copy()
            
            # 회사명 영문명 적용 (시각화용)
            filtered_df['short_display_name'] = filtered_df.apply(
                lambda r: f"{get_english_company_name(r['회사명'])}*" if not r['is_fallback'] else get_english_company_name(r['회사명']),
                axis=1
            )
            
            # 색상 설정 (연한 색, 진한 색)
            color_sets = {
                '생명보험': ['#A6CEE3', '#1F78B4'], 
                '손해보험': ['#FDBF6F', '#FF7F00'],
                '재보험': ['#B2DF8A', '#33A02C']  # 연한 초록, 진한 초록
            }

            # 차트 렌더링 로직 (상단 2열: 생명/손해, 하단 1열: 재보험)
            col_l, col_r = st.columns(2)
            for i, sector in enumerate(['생명보험', '손해보험']):
                target_col = col_l if i == 0 else col_r
                with target_col:
                    sector_df = company_df[company_df['구분'] == sector]
                    sum_num = sector_df['eff_num'].sum()
                    sum_den = sector_df['eff_den'].sum()
                    weighted_avg = (sum_num / sum_den * 100) if sum_den > 0 else 0
                    
                    render_sector_chart(sector, filtered_df, company_df, color_sets, weighted_avg)
            
            st.divider()
            # 재보험 차트 (상단 차트와 width를 맞추기 위해 2컬럼 레이아웃 사용)
            col_re, col_empty = st.columns(2)
            with col_re:
                sector = '재보험'
                sector_df = company_df[company_df['구분'] == sector]
                sum_num = sector_df['eff_num'].sum()
                sum_den = sector_df['eff_den'].sum()
                weighted_avg = (sum_num / sum_den * 100) if sum_den > 0 else 0
                render_sector_chart(sector, filtered_df, company_df, color_sets, weighted_avg)
            
            with st.expander("📍 상세 데이터 확인"):
                # 표시용 데이터프레임 구성
                display_df = filtered_df.copy()
                display_df['영문회사명'] = display_df['회사명'].map(get_english_company_name)
                # A, D 컬럼이 없을 수 있으므로 안전하게 처리
                for col in ['A', 'D']:
                    if col not in display_df.columns:
                        display_df[col] = 0
                
                st.dataframe(display_df[['구분', '회사명', '영문회사명', 'D', 'A', 'final_ratio', 'is_fallback']].rename(
                    columns={
                        'D': '비율(경과후)', 
                        'A': '비율(경과전)',
                        'final_ratio': '지급여력비율(%)', 
                        'is_fallback': '경과전사용여부'
                    }
                ), width="stretch")
        else:
            st.warning(f"{selected_month}에 대한 데이터가 없습니다. 먼저 데이터 수집을 진행해 주세요.")
    else:
        st.warning("표시할 회사별 데이터가 없습니다. 먼저 '데이터 수집기' 탭에서 데이터를 수집해 주세요.")

elif selected_tab == "📉 회사별 변동 (Company Change)":
    st.subheader("📉 회사별 K-ICS 변동 (최근 분기 vs 직전 분기)")

    available_months = get_available_months()
    if len(available_months) < 2:
        st.warning("최근/직전 분기 비교를 위해 최소 2개 분기 데이터가 필요합니다.")
    else:
        latest_month = available_months[0]
        previous_month = available_months[1]
        st.markdown(f"**비교 기준**: 최근 `{latest_month}` vs 직전 `{previous_month}`")

        current_df, _ = load_company_solvency_data(latest_month)
        previous_df, _ = load_company_solvency_data(previous_month)

        if current_df.empty or previous_df.empty:
            st.warning("비교에 필요한 회사별 데이터가 부족합니다. 데이터 수집 후 다시 시도해주세요.")
        else:
            current_df = apply_sector_reclassification(current_df)
            previous_df = apply_sector_reclassification(previous_df)
            change_df = build_company_change_df(current_df, previous_df)

            if change_df.empty:
                st.warning("두 분기 모두 존재하는 회사 데이터가 없어 변동을 계산할 수 없습니다.")
            else:
                change_df['english_name'] = change_df['company_name'].map(get_english_company_name).fillna("")
                before_filter_count = len(change_df)
                change_df = change_df[change_df['english_name'].astype(str).str.strip() != ""].copy()
                filtered_out = before_filter_count - len(change_df)

                if change_df.empty:
                    st.warning("영문 회사명이 있는 회사가 없어 차트를 표시할 수 없습니다.")
                else:
                    if filtered_out > 0:
                        st.caption(f"영문 회사명이 없는 회사 {filtered_out}개는 제외되었습니다.")

                    sector_order = ['생명보험', '손해보험', '재보험']

                    row1_cols = st.columns(3)
                    for idx, sector in enumerate(sector_order):
                        with row1_cols[idx]:
                            render_company_change_chart(
                                change_df,
                                sector,
                                'delta_before',
                                f"{sector} - 경과조치 반영 전 증감",
                                f"{sector}_before"
                            )

                    row2_cols = st.columns(3)
                    for idx, sector in enumerate(sector_order):
                        with row2_cols[idx]:
                            render_company_change_chart(
                                change_df,
                                sector,
                                'delta_after',
                                f"{sector} - 경과조치 반영 후 증감",
                                f"{sector}_after"
                            )

                with st.expander("상세 데이터 확인"):
                    detail_df = change_df[[
                        'sector', 'company_name', 'english_name',
                        'ratio_before_previous', 'ratio_before_current', 'delta_before',
                        'ratio_after_previous', 'ratio_after_current', 'delta_after'
                    ]].copy()
                    detail_df['delta_before'] = detail_df['delta_before'].round(1)
                    detail_df['delta_after'] = detail_df['delta_after'].round(1)
                    st.dataframe(
                        detail_df.sort_values(['sector', 'delta_after'], ascending=[True, False]),
                        width="stretch"
                    )

elif selected_tab == "📡 데이터 수집기 (Collector)":
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
                    st.dataframe(df_pivot, width="stretch")
                    
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
                    st.dataframe(df, width="stretch")
                
                # 수집이 완료되었으니 화면 갱신을 유도하거나 정보를 제공
                st.info("💡 새로운 데이터가 저장되었습니다. '분석 대시보드' 탭으로 이동하여 차트를 확인해 보세요.")
            else:
                st.warning("수집된 데이터가 없습니다. API Key나 기준년월을 확인해주세요.")
