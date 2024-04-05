-- part of a query repo
-- query name: XMTP daily wallet activations
-- query link: https://dune.com/queries/3546280


WITH activations AS (
  SELECT
    DATE_TRUNC('day', timestamp) AS day,
    COUNT(DISTINCT wallet_address) AS wallet_activations
  FROM dune.xmtp_team.dataset_xmtp_activated_addresses
  GROUP BY DATE_TRUNC('day', timestamp)
),
moving_averages AS (
  SELECT
    day,
    wallet_activations,
    AVG(wallet_activations) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_average_7,
    AVG(wallet_activations) OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS moving_average_30,
    AVG(wallet_activations) OVER (ORDER BY day ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS moving_average_90
  FROM activations
),
cumulative_activations AS (
  SELECT
    *,
    SUM(wallet_activations) OVER (ORDER BY day) AS wallets_activated
  FROM moving_averages
)
SELECT
  *
FROM cumulative_activations
WHERE day >= DATE_ADD('day', -180, CURRENT_DATE)
ORDER BY day