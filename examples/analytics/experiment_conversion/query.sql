SELECT variant,
       COUNT(*) AS participants,
       SUM(CASE WHEN converted = 1 THEN 1 ELSE 0 END) AS conversions,
       SUM(CASE WHEN converted = 0 THEN 1 ELSE 0 END) AS non_conversions
FROM events
GROUP BY variant
ORDER BY variant;
