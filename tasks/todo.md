# 작업 계획

- [x] 수집 로직과 현재 저장 경로 확인
- [x] 경과조치 후 누락값 보정 함수 추가
- [x] 수집 저장/표시 경로에 보정 적용
- [x] 검증 및 diff/비밀정보 점검
- [x] 커밋 및 푸시

## 검토 결과

- 경과조치 후 계정코드(D/E/F) 값이 없으면 동일 회사/월의 경과조치 전 계정코드(A/B/C) 값을 사용하도록 보정했습니다.
- API가 경과조치 후 행을 아예 반환하지 않는 경우에도 보정 행을 생성해 MotherDuck 저장 대상에 포함합니다.
- 테스트: `python3 -c "import py_compile; py_compile.compile('/Users/hyunsikhwang/collect-fisis/app.py', cfile='/private/tmp/collect_fisis_app.pyc', doraise=True)"` (pass)
- 테스트: 표준 테스트 스크립트 없음 (`Makefile`, `package.json`, `pyproject.toml`, `pytest.ini` 없음)
- 점검: `git diff --check` (pass)
- 점검: 비밀정보 스캔 결과 신규 비밀값 없음
