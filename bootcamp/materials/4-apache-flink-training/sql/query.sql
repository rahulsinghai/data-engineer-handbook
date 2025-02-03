SELECT
  geodata::json->>'country' as country,
  geodata::json->>'city' as city,
  COUNT(1) as num_events
FROM processed_events
GROUP BY 1, 2
LIMIT 10;