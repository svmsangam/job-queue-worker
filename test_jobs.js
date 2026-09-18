import http from 'k6/http';
import { check } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

export const options = {
  scenarios: {
    constant_request_rate: {
      executor: 'constant-arrival-rate',
      rate: 100,             // 100 requests per second
      timeUnit: '1s',
      duration: '30s',        // Total 3,000 requests
      preAllocatedVUs: 20,
      maxVUs: 50,
    },
  },
};

const JOB_TYPES = ['IMAGE_RESIZE', 'PDF_GENERATE', 'EMAIL_NOTIFICATION', 'DATA_EXPORT'];

export default function () {
  const jobId = `job-${uuidv4()}`;
  const jobType = JOB_TYPES[Math.floor(Math.random() * JOB_TYPES.length)];

  const payload = JSON.stringify({
    id: jobId,
    type: jobType,
    payload: {
      user_id: Math.floor(Math.random() * 10000),
      timestamp: new Date().toISOString(),
      metadata: `test-batch-${Math.floor(Math.random() * 100)}`,
    },
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
    },
  };

  const res = http.post('http://localhost:8080/v1/jobs', payload, params);

  check(res, {
    'status is 202': (r) => r.status === 202,
  });
}