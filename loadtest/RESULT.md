# 신뢰성 부하 / 스트레스 테스트 결과

`reliable-webhook-dispatcher`가 처음 설계 목표 — **유실 0 · 중복 0 · 장애에도 수렴** — 을 실제로 지키는지, docker compose 전체 스택에 부하·장애를 주입해 실측한 기록.

> 처리량·latency 수치는 측정 환경(로컬 macOS / Docker) 의존이라 절대치로 단언하지 않는다.
> **구조적 사실(유실0 · 중복0 · 장애 후 수렴)만 단언**한다.

## 환경

- 스택: docker compose (postgres:17 + app + prometheus + grafana)
- worker: poll 2s · batch 10 · max_attempts 5 · lease 1m (학습 기본값)
- 부하: k6 (`loadtest/order.js`, `POST /orders`)

## 결과 요약

| 시나리오 | 부하 | 검증 항목 | 결과 |
|---|---|---|---|
| **정상** | 300건 (vus 50) | 유실0 / 중복0 / 생성 처리량 | sent **+300**, mock distinct **300** · 중복 **0** / **884 req/s**, p95 **125ms** |
| **5xx 장애** (transient) | 50건 | 자동 재시도 복구, 유실0 | 장애 중 50건 `PENDING` 보존 → 복구 후 **자동 sent +50**, failed 0 |
| **4xx 장애** (permanent) | 20건 | dead-letter 격리 + 수동 replay, 유실0 | 20건 `FAILED` 격리 → `replay` → **sent +20**, 멱등 재replay **0** |
| **통합테스트** | N워커 동시 claim | 동시성 중복0, 데이터레이스0 | `RUN_DB_TESTS=1 TEST_DATABASE_URL=... go test -race ./...` 30건 전부 통과 **ok** |

## 단언 / 비단언

- **단언(구조적)**: 던진 주문 = 최종 수렴(유실0) · 동시 다중 워커 중복 claim 0 · 외부 장애(5xx/4xx) 후 한 건도 안 잃고 수렴.
- **비단언(환경 의존)**: 생성 처리량 884 req/s, p95 125ms는 *이 환경의 측정값*일 뿐. 전송 처리량은 worker 설정(batch 10 / poll 2s)에 종속.

## 재현

```bash
docker compose up --build -d

# 정상 (유실0 / 중복0)
k6 run loadtest/order.js
curl -s localhost:8080/outbox/stats          # sent 증가분 == 던진 수
curl -s localhost:8080/mock/webhook/received # distinct == 던진 수, 중복 없음

# 5xx 장애 → 자동 복구
WEBHOOK_TARGET_URL='http://localhost:8080/mock/webhook?mode=fail' docker compose up -d --no-deps app
k6 run -e N=50 loadtest/order.js             # 전송 실패 → PENDING 에 보존
WEBHOOK_TARGET_URL='http://localhost:8080/mock/webhook' docker compose up -d --no-deps app   # 복귀 → 자동 수렴

# 4xx 장애 → dead-letter → 수동 replay
WEBHOOK_TARGET_URL='http://localhost:8080/mock/webhook?mode=bad' docker compose up -d --no-deps app
k6 run -e N=20 loadtest/order.js             # 즉시 FAILED
curl -s localhost:8080/admin/dead-letters    # 격리된 목록
WEBHOOK_TARGET_URL='http://localhost:8080/mock/webhook' docker compose up -d --no-deps app
curl -X POST localhost:8080/admin/dead-letters/replay   # 복구 → sent
```
