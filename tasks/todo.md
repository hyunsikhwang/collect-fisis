# 작업 계획

- [x] Streamlit Cloud 유지 조건에서 가능한 UI 개선 방향 재정의
- [x] 메인 탭을 `st.radio`에서 HTML 링크 네비게이션으로 교체
- [x] Pretendard/간격/차트 chrome을 더 절제된 대시보드 톤으로 재정리
- [x] 로컬 렌더/문법/비밀정보 검증
- [x] 커밋 및 푸시

## 검토 결과

- Streamlit Cloud 제약 때문에 프론트엔드 분리는 제외하고, Streamlit 내에서 통제 가능한 구조로 재정리했습니다.
- 메인 네비게이션은 Streamlit 라디오 위젯 대신 쿼리파라미터 기반 HTML 링크로 교체해 줄바꿈/위젯 스타일 오염을 줄였습니다.
- Pretendard를 유지하면서 헤더/네비게이션/섹션/패널 간격을 더 조밀하고 미니멀하게 맞췄습니다.
- pyecharts 기본 toolbox를 숨기고 차트 제목/축/범례 폰트와 팔레트를 더 절제된 톤으로 맞췄습니다.
- 검증: `py_compile` (pass)
- 검증: `git diff --check` (pass)
- 검증: pyecharts 옵션 생성 스모크 테스트 (pass)
- 검증: Streamlit 서버 기동 및 HTTP 응답 확인 (pass)
- 브라우저 스크린샷 검증은 Chrome/Playwright 환경 제약으로 완전 수행하지 못했습니다. Chrome headless 캡처는 Streamlit 초기 skeleton까지만 확인했습니다.
- 테스트: 미실행 (사유: 표준 테스트 스크립트 없음)
