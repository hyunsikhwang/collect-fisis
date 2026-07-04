# 작업 계획

- [x] 현재 Streamlit UI 구조와 스타일 삽입 지점 확인
- [x] Pretendard 강제 적용 및 불필요한 헤더/메타 문구 제거
- [x] 탭 줄바꿈과 expander 아이콘 깨짐 해결
- [x] 검증 및 diff/비밀정보 점검
- [x] 커밋 및 푸시

## 검토 결과

- Pretendard Variable을 최상위 폰트로 강제하고, Material icon 폰트는 예외 처리했습니다.
- 사용자가 지적한 헤더 subtitle, FISIS/MotherDuck 메타 문구, 섹션 설명 문구를 제거했습니다.
- 모바일 탭이 줄바꿈되지 않도록 nowrap/폭/패딩을 재조정했습니다.
- 깨져 보이던 Streamlit expander 기반 UI를 주요 화면에서 제거하고 border container 기반 패널로 정리했습니다.
- 브라우저 검증: 임시 Python 3.12 venv에서 `streamlit run` 실행 후 데스크톱 1440x1000, 모바일 390x844 렌더 확인 (pass)
- 테스트: `python3 -c "import py_compile; py_compile.compile('/Users/hyunsikhwang/collect-fisis/app.py', cfile='/private/tmp/collect_fisis_app.pyc', doraise=True)"` (pass)
- 테스트: 미실행 (사유: 표준 테스트 스크립트 없음)
- 점검: `git diff --check` (pass)
- 점검: 비밀정보 스캔 결과 신규 비밀값 없음
