# Reliable Webhook Dispatcher

도메인 이벤트를 외부로 **잃지 않고, 추적 가능하게, 재처리 가능하게** 보내려면 무엇이 필요한지 직접 만들어 본 Go 웹훅 디스패처.

주문 생성 같은 도메인 이벤트와 "보낼 이벤트"를 하나의 DB 트랜잭션에 묶어 두고(outbox), 백그라운드 워커가 그걸 집어서 webhook 으로 보낸다. 외부가 죽어도 이벤트는 남아 있고, 실패는 기록되고 다시 시도되며, 끝내 안 되면 dead-letter 로 남아 나중에 다시 밀 수 있다.

![Grafana — 부하 중 Outbox Backlog가 솟았다 worker가 소비하며 빠지는 톱니, sent 전이율](assets/grafana-dashboard.png)

## 무엇을 보장하나

| 목표 | 방법 | 검증 |
|---|---|---|
| **유실 0** | order와 outbox event를 같은 트랜잭션에 커밋 (dual-write 제거) | 정상 300 / 5xx 50 / 4xx 20건 부하 — 던진 주문 전부 수렴 |
| **중복 0 (내부 상태)** | `FOR UPDATE SKIP LOCKED` claim + `claim_token` fencing | 동시 N워커 claim 중복 0 (`go test -race`) |
| **중복 0 (외부 효과)** | `event_id`를 `Idempotency-Key`로 송신 + receiver dedup | mock distinct == 던진 수 |
| **추적 가능** | 모든 전송 시도 `delivery_attempts` 기록 + Prometheus + 구조화 로그 | `/metrics`, Grafana 대시보드 |
| **포기 0** | 재시도/backoff → `FAILED` dead-letter → 운영자 replay | 4xx 20건 FAILED → replay로 전부 복구 |

## 왜 만들었나

전 직장에서 외부로 알림과 이벤트를 발송하면서 세 가지에 막혔다. 외부가 죽으면 발송이 통째로 유실됐고, 발송 여부를 확인할 이력이 없어 로그를 뒤져야 했으며, 재시도는 막연히 붙어 있어 왜·몇 번 실패했는지 알 수 없었다. 전송이 개발자의 제어 밖에 있었다.

그때는 못 고치고 넘어왔다. 이 프로젝트는 그 문제를 다시 만들어 놓고, 어떻게 하면 됐을까를 하나씩 확인해 본 결과다.

메시지 브로커를 얹으면 많은 게 덮이는데, 그러면 정작 무엇이 문제였는지를 못 배울 것 같았다. 그래서 브로커 없이 갔다 — 쓰기를 어디까지 한 트랜잭션에 묶을지, 외부 호출을 어디로 뺄지, 실패를 어떻게 남길지를 직접 정해 보는 게 목적이었다.

## 아키텍처

```text
POST /orders ──(한 트랜잭션)──> orders + outbox_events(PENDING)
                                       │
        worker pool ──claim (SKIP LOCKED + claim_token)──> PROCESSING
                                       │
                          webhook 전송 (트랜잭션 밖)
                       ┌───────────────┼───────────────┐
                      2xx          5xx / timeout       4xx
                       │               │                │
                      SENT       PENDING(재시도+backoff)  FAILED(dead-letter)
                                                          │
                                                  replay → PENDING
```

- 상태는 `PENDING → PROCESSING → SENT / PENDING / FAILED` 5개만 사용한다.
- 여러 워커가 동시에 같은 이벤트를 집지 않도록 `FOR UPDATE SKIP LOCKED`로 claim한다.
- claim 트랜잭션이 끝난 뒤(락 공백) 늦게 돌아온 워커의 상태 덮어쓰기는 `claim_token`으로 차단한다.
- 워커가 전송 도중 죽어 `PROCESSING`에 묶인 이벤트는 lease 초과 시 recovery가 `PENDING`으로 회수한다.

## 신뢰성 검증

부하·스트레스 테스트로 위 보장을 실측했다 (상세 · 재현 절차: [loadtest/RESULT.md](loadtest/RESULT.md)).

| 시나리오 | 결과 |
|---|---|
| 정상 300건 | sent +300, 중복 0, 생성 884 req/s (p95 125ms) |
| 5xx 장애 50건 | 전송 실패분이 `PENDING`으로 보존 → 복구 후 자동 수렴, 유실 0 |
| 4xx 장애 20건 | `FAILED` dead-letter → replay로 전부 복구 (멱등) |

> 처리량 수치는 측정 환경에 따라 다르니 단언하지 않는다. 단언하는 건 구조적 사실 — 던진 건 전부 수렴하고, 같은 건 두 번 가지 않고, 외부가 죽어도 잃지 않는다.

![GitHub Actions — unit / integration 통과](assets/ci-pass.png)

### fencing 이 실제로 막는지

`claim_token` 은 이 프로젝트에서 제일 설명하기 어려운 부분이라, 테스트가 그 설명을 대신하게 뒀다.

`SKIP LOCKED` 는 두 워커가 **동시에** 같은 행을 집는 걸 막는다. 그런데 claim 트랜잭션은 짧게 끝나고 webhook 전송은 그 밖에서 수 초간 일어난다. **전송 중에는 락이 없다.** 그 사이에 워커가 멈추면 lease 가 지나 시스템이 회수하고, 다른 워커가 같은 이벤트를 가져간다. 멈췄던 워커가 뒤늦게 깨어나 결과를 쓰면 이미 끝난 걸 덮는다.

```
t0  A 가 claim            → PROCESSING, token = TA
t1  A 가 전송 중 멈춤
t2  lease 초과 → 회수      → PENDING, token = NULL
t3  B 가 claim            → PROCESSING, token = TB
t4  A 가 깨어나 결과를 쓴다  → WHERE claim_token = TA → 0 rows → 거부
```

`TestOutboxStore_ReclaimedByAnotherWorker_staleWriteRejectedAndWinnerKept` 가 이 흐름을 그대로 돌린다. 중요한 건 t3 이 있다는 것 — B 가 **실제로 재claim해서 자기 토큰을 들고 있는 상태**에서 A 를 시도한다. 그리고 A 가 막힌 뒤 B 는 통과하는지까지 본다. 무효한 것만 막고 유효한 건 통과시켜야 fencing 이지, 둘 다 막으면 그냥 고장이다.

이 테스트가 값을 하는지 확인하려고 `claim_token` 조건만 빼고 돌려 봤다. 그러면 A 의 쓰기가 1 행을 바꾸며 실패한다. 반면 회수 직후(`claim_token = NULL`) 만 보는 기존 테스트는 조건을 빼도 통과한다 — 그 경우엔 `status` 가 이미 `PENDING` 이라 fencing 이 아니라 상태 조건이 막고 있었기 때문이다. **통과하는 테스트가 늘 그 이유로 통과하는 건 아니라는 걸 여기서 봤다.**

## 보장하지 않는 것

- **exactly-once delivery.** at-least-once로 보내고 `event_id`를 `Idempotency-Key`로 실어 받는 쪽이 거르게 한다. '전송'과 '전송했음 기록(SENT)'이 또 dual-write라 그 사이에 죽으면 중복이 나간다. 최종 dedup은 받는 쪽 몫이고, 이 프로젝트는 그 가정을 mock으로 관측만 했다
- **처리량 절대치.** 측정 하네스까지만 만들었다. 나온 숫자는 내 노트북 기준이라 환경을 함께 적지 않으면 의미가 없다
- **운영 검증.** 부하·장애 주입은 전부 로컬에서 돌렸다. 실제 트래픽 아래에서 이 구조가 맞았는지를 말해 줄 근거는 아직 없다
- **lease timeout·backoff·워커 수는 근거 있는 값이 아니다.** 환경이 정하는 값인데 여기선 학습 기본값을 썼다. 다만 어디를 보고 정해야 하는지는 안다

## API

| 메서드 · 경로 | 용도 |
|---|---|
| `POST /orders` | 주문 생성 + outbox 이벤트 (`Idempotency-Key` 지원) |
| `GET /outbox` · `GET /outbox/stats` | outbox 조회 / 상태별 집계 |
| `GET /delivery-attempts` | 전송 시도 이력 |
| `GET /admin/dead-letters` | FAILED(dead-letter) 목록 |
| `POST /admin/dead-letters/replay` | 전체 FAILED replay |
| `POST /admin/outbox/{event_id}/replay` | 단건 replay |
| `GET /metrics` | Prometheus 메트릭 |
| `GET /health` · `GET /ready` | 헬스 / 레디니스 |

## 실행

```bash
docker compose up --build -d   # postgres + migrate + app + prometheus + grafana
curl localhost:8080/health     # {"status":"ok"}
```

- app: http://localhost:8080
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (대시보드 자동 등록)

부하·장애 주입 데모 절차는 [loadtest/RESULT.md](loadtest/RESULT.md) 참고.

## 기술 스택

Go 1.25 · PostgreSQL 17 · `database/sql` + `pgx` · 표준 `net/http` · `log/slog` · Prometheus · Grafana · Docker Compose · GitHub Actions

## 테스트

```bash
go test -race ./...                                       # 단위 (통합은 skip)

# 통합까지 돌리려면 RUN_DB_TESTS 가 있어야 한다. 없으면 조용히 skip 된다.
RUN_DB_TESTS=1 TEST_DATABASE_URL=postgres://... go test -race ./...   # 30건 전부
k6 run loadtest/order.js                           # 부하
```

## 프로젝트 구조

```text
main.go              앱 조립 + 생명주기(graceful shutdown)
internal/config      환경 설정
internal/httpapi     HTTP 서버·핸들러·mock receiver·미들웨어
internal/store       orders·outbox·delivery·idempotency
internal/worker      dispatcher·recovery
internal/metrics     Prometheus 메트릭
migrations           PostgreSQL 스키마
deploy               prometheus·grafana provisioning
loadtest             k6 부하 스크립트 + 결과(RESULT.md)
.github/workflows    CI (unit + integration)
```
