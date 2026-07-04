# 작업 계획

- [x] 현재 Collector 캐시/수집 흐름 확인
- [x] 기존 동일 기간 데이터 발견 시 overwrite 확인 UI 추가
- [x] overwrite 선택 시 기존 월 데이터 삭제 후 전체 재수집 적용
- [x] 검증 및 diff/비밀정보 점검
- [x] 커밋 및 푸시

## 검토 결과

- Collector 탭에서 동일 기준년월 캐시가 있으면 기존 데이터 유지 또는 덮어쓰기를 선택하도록 UI를 추가했습니다.
- 기본값은 기존 데이터 유지이며, 덮어쓰기 선택 시 기존 월 데이터는 API 재수집 결과가 준비된 뒤 삭제/교체됩니다.
- 테스트: `python3 -c "import py_compile; py_compile.compile('/Users/hyunsikhwang/collect-fisis/app.py', cfile='/private/tmp/collect_fisis_app.pyc', doraise=True)"` (pass)
- 테스트: 미실행 (사유: 표준 테스트 스크립트 없음)
- 점검: `git diff --check` (pass)
- 점검: 비밀정보 스캔 결과 신규 비밀값 없음
