# Reliable Webhook Dispatcher

Outbox 패턴 기반 웹훅 디스패처 학습용 Go 프로젝트입니다.

주문 생성 같은 도메인 이벤트를 DB 트랜잭션 안에서 `outbox_events` 테이블에 저장하고, 별도 워커가 이벤트를 안정적으로 전송하는 구조를 목표로 합니다. 현재 구현 범위는 애플리케이션 부트스트랩, PostgreSQL 연결, graceful shutdown, 헬스체크 API, outbox 관련 초기 스키마입니다.

## Stack

- Go 1.25.1
- PostgreSQL 17
- pgx
- godotenv

## Current Features

- `.env` 기반 설정 로드
- PostgreSQL 연결 확인
- HTTP 서버 실행
- `GET /health` 헬스체크
- SIGINT/SIGTERM 기반 graceful shutdown
- orders, outbox_events, delivery_attempts 초기 스키마

## Run

`.env`를 준비합니다.

```env
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=postgres
DATABASE_URL=postgres://postgres:postgres@localhost:55432/postgres?sslmode=disable
HTTP_ADDR=:8080
```

환경 변수:

- `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`: Docker Compose PostgreSQL 설정
- `DATABASE_URL`: 애플리케이션이 접속할 PostgreSQL DSN
- `HTTP_ADDR`: HTTP 서버 바인딩 주소

PostgreSQL을 실행합니다.

```bash
docker compose up -d
```

마이그레이션을 적용합니다.

```bash
psql "$DATABASE_URL" -f migrations/000001_init.up.sql
```

애플리케이션을 실행합니다.

```bash
go run .
```

정상 실행되면 DB 연결 성공 로그와 HTTP 서버 시작 로그가 출력됩니다.

## API

### Health Check

```bash
curl http://localhost:8080/health
```

응답:

```json
{ "status": "ok" }
```

## Database

초기 마이그레이션은 다음 테이블을 생성합니다.

- `orders`: 주문 데이터 예시 테이블
- `outbox_events`: 전송해야 할 도메인 이벤트 저장소
- `delivery_attempts`: 웹훅 전송 시도 이력

주요 outbox 상태:

- `PENDING`: 전송 대기
- `PROCESSING`: 워커가 처리 중
- `SENT`: 전송 성공
- `FAILED`: 전송 실패

롤백이 필요하면 down 마이그레이션을 실행합니다.

```bash
psql "$DATABASE_URL" -f migrations/000001_init.down.sql
```

## Test

```bash
go test ./...
```

현재 테스트는 HTTP health handler를 검증합니다.

## Project Structure

```text
.
├── main.go                    # 앱 부트스트랩, DB 연결, graceful shutdown
├── internal/config            # 환경 변수 설정
├── internal/httpapi           # HTTP 서버와 핸들러
├── migrations                 # PostgreSQL 스키마
└── docker-compose.yml         # 로컬 PostgreSQL
```
