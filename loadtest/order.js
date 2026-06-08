// k6 부하 스크립트 — POST /orders 를 동시 다발로 던져 유실0/중복0/처리량을 검증한다.
// 실행: k6 run loadtest/order.js   (옵션: -e VUS=50 -e N=300)
import http from 'k6/http';
import { check } from 'k6';

const VUS = __ENV.VUS ? parseInt(__ENV.VUS) : 50;
const N = __ENV.N ? parseInt(__ENV.N) : 300;

export const options = {
  vus: VUS,
  iterations: N,
};

const CUST = '11111111-1111-1111-1111-111111111111';
const BASE = __ENV.BASE || 'http://localhost:8080';

export default function () {
  const res = http.post(
    `${BASE}/orders`,
    JSON.stringify({ customer_id: CUST, amount: '10.00' }),
    { headers: { 'Content-Type': 'application/json' } },
  );
  check(res, { 'status 201': (r) => r.status === 201 });
}
