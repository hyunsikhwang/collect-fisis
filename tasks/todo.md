# 작업 계획

- [x] Collector/API 호출 구조 확인
- [x] 현재 날짜 기준 최근 3개 완료 분기말 산출 로직 추가
- [x] FSS API 샘플 프로브로 최신 업데이트 기준년월 확인
- [x] Collector UI에 API 업데이트 상태 표시 추가
- [x] 검증 및 비밀정보 점검
- [x] 커밋 및 푸시

## 검토 결과

- 최근 3개 완료 분기말 후보를 KST 현재 날짜 기준으로 산출합니다.
- Collector에서 API Key가 있으면 최근 후보를 샘플 조회해 가장 최신 업데이트 기준년월을 표시합니다.
- 전체 수집과 분리된 가벼운 프로브로 구현해 기존 수집/캐시/overwrite 흐름은 유지했습니다.
- 검증: `py_compile` (pass)
- 검증: `git diff --check` (pass)
- 검증: Streamlit 서버 기동 및 Collector HTTP 응답 확인 (pass)
- 점검: 비밀정보 스캔 결과 신규 비밀값 없음
- 테스트: 미실행 (사유: 표준 테스트 스크립트 없음)
