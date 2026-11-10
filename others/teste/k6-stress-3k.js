import http from 'k6/http';
import { check } from 'k6';
import { uuidv4 } from './k6-utils.js';

export const options = {
  scenarios: {
    stress_3000_rps: {
      executor: 'constant-arrival-rate',
      rate: 3000,
      timeUnit: '1s',
      duration: '20s',
      preAllocatedVUs: 300,
      maxVUs: 1500,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.05'],
  },
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
};

const params = {
  headers: {
    'Content-Type': 'application/json',
  },
  timeout: '3000ms',
};

export default function () {
  const payload = JSON.stringify({
    correlationId: uuidv4(),
    amount: 19.90,
  });

  const res = http.post('http://localhost:9999/payments', payload, params);

  check(res, {
    'status is 200': (r) => r.status === 200,
  });
}

