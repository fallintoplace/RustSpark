SELECT service,
       COUNT(*) AS requests,
       SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS errors,
       SUM(CASE WHEN latency_ms > 1000 THEN 1 ELSE 0 END) AS slow_requests,
       MAX(latency_ms) AS max_latency_ms
FROM events
GROUP BY service
ORDER BY service;
