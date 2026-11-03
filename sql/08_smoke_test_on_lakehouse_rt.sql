-- 08. Smoke test on Lakehouse//RT
--
-- The final proof that Lakehouse//RT can read each table. Run it on poc-rt, once for each table.
-- If a table uses something the Beta doesn't support, the error names it.

SELECT * FROM <catalog>.<schema>.<table> LIMIT 10;

-- Then run one real dashboard query on poc-rt: get its full text with the second query in 06_measure_queries.sql.
-- Lakehouse//RT runs SQL in ANSI mode only, so this also catches queries that relied on lenient casts,
-- which raise errors instead of returning NULL.
