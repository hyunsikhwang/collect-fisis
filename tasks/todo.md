# 작업 계획

- [x] 현재 Streamlit UI 구조와 스타일 삽입 지점 확인
- [x] 공통 디자인 토큰/CSS와 앱 헤더 적용
- [x] Trend/Snapshot/Changes/Collector 섹션의 헤더, 컨트롤, 차트 스타일 정리
- [x] 검증 및 diff/비밀정보 점검
- [x] 커밋 및 푸시

## 검토 결과

- 공통 디자인 토큰/CSS, 앱 헤더, 섹션 헤더를 추가해 기존 Streamlit 기본 스타일을 정리했습니다.
- 외부 탭 컴포넌트 대신 네이티브 Streamlit 선택 컨트롤을 탭처럼 스타일링해 동적 JS 로드 실패 위험을 줄였습니다.
- 더 이상 사용하지 않는 `streamlit-shadcn-ui` 의존성을 제거했습니다.
- Trend/Snapshot/Changes/Collector의 기능 구조는 유지하고, 라벨/버튼/차트 팔레트를 모던한 금융 대시보드 톤으로 정리했습니다.
- `.streamlit/secrets.toml`이 없는 환경에서도 Collector/ECOS 설정 조회가 앱을 중단하지 않도록 `get_secret()` 경로로 통일했습니다.
- 브라우저 검증: 임시 Python 3.12 venv에서 `streamlit run` 실행 후 데스크톱 1440x1000, 모바일 390x844 렌더 확인 (pass)
- 테스트: `python3 -c "import py_compile; py_compile.compile('/Users/hyunsikhwang/collect-fisis/app.py', cfile='/private/tmp/collect_fisis_app.pyc', doraise=True)"` (pass)
- 테스트: 미실행 (사유: 표준 테스트 스크립트 없음)
- 점검: `git diff --check` (pass)
- 점검: 비밀정보 스캔 결과 신규 비밀값 없음
