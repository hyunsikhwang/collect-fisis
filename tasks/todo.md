# 작업 계획

- [x] Trend 데이터 집계 지점 확인
- [x] 회사별 피벗 후 경과조치 후 0/null 보정 헬퍼 추가
- [x] 업권 Trend와 회사별 시계열에 보정 적용
- [x] 검증 및 diff/비밀정보 점검
- [x] 커밋 및 푸시

## 검토 결과

- Trend 업권 합계 전에 회사별 피벗 데이터의 경과조치 적용 후 지급여력금액/기준금액이 0 또는 null이면 각각 경과조치 적용 전 금액/기준금액으로 보정합니다.
- 회사별 시계열 Trend도 동일한 보정 함수를 적용해 업권 Trend와 일관되게 계산합니다.
- 테스트: `python3 -c "import py_compile; py_compile.compile('/Users/hyunsikhwang/collect-fisis/app.py', cfile='/private/tmp/collect_fisis_app.pyc', doraise=True)"` (pass)
- 테스트: 미실행 (사유: 표준 테스트 스크립트 없음)
- 점검: `git diff --check` (pass)
- 점검: 비밀정보 스캔 결과 신규 비밀값 없음
