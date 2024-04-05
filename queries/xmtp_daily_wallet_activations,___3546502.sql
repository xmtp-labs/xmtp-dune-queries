-- part of a query repo
-- query name: XMTP daily wallet activations, by provider (ex-CB)
-- query link: https://dune.com/queries/3546502


WITH ens_addresses AS (
  SELECT DISTINCT owner AS unique_address
  FROM ens.view_registrations
),
lens_addresses AS (
  SELECT DISTINCT "to" AS unique_address
  FROM lens_polygon.LensHub_evt_Transfer
),
ud_addresses AS (
  SELECT DISTINCT "to" AS unique_address
  FROM unstoppabledomains_ethereum.UNSRegistry_evt_Transfer
  UNION
  SELECT DISTINCT "to" AS unique_address
  FROM unstoppabledomains_polygon.UNSRegistry_evt_Transfer
),
fc_addresses AS (
  SELECT DISTINCT FROM_HEX(TRY_CAST(JSON_EXTRACT(claim, '$.address') AS VARCHAR)) AS unique_address
  FROM dune.neynar.dataset_farcaster_verifications
),
combined_addresses AS (
  SELECT unique_address FROM ens_addresses
  UNION
  SELECT unique_address FROM lens_addresses
  UNION
  SELECT unique_address FROM ud_addresses
  UNION
  SELECT unique_address FROM fc_addresses
),
formatted_addresses AS (
  SELECT
    DATE_TRUNC('day', timestamp) AS day,
    FROM_HEX(REPLACE(wallet_address, '/')) as wallet_address
  FROM dune.xmtp_team.dataset_xmtp_activated_addresses
  WHERE LENGTH(REPLACE(wallet_address, '/')) = 42
),
owners AS (
  SELECT
    day,
    COUNT(DISTINCT CASE WHEN wallet_address IN (SELECT unique_address FROM ens_addresses) THEN wallet_address END) AS ens_owners,
    COUNT(DISTINCT CASE WHEN wallet_address IN (SELECT unique_address FROM lens_addresses) THEN wallet_address END) AS lens_owners,
    COUNT(DISTINCT CASE WHEN wallet_address IN (SELECT unique_address FROM ud_addresses) THEN wallet_address END) AS ud_owners,
    COUNT(DISTINCT CASE WHEN wallet_address IN (SELECT unique_address FROM fc_addresses) THEN wallet_address END) AS fc_owners,
    COUNT(DISTINCT CASE WHEN wallet_address IN (SELECT unique_address FROM combined_addresses) THEN wallet_address END) AS combined_owners
  FROM formatted_addresses
  GROUP BY day
),
final AS (
  SELECT
    day,
    ens_owners,
    SUM(ens_owners) OVER (ORDER BY day) AS cumulative_ens_owners,
    lens_owners,
    SUM(lens_owners) OVER (ORDER BY day) AS cumulative_lens_owners,
    ud_owners,
    SUM(ud_owners) OVER (ORDER BY day) AS cumulative_ud_owners,
    fc_owners,
    SUM(fc_owners) OVER (ORDER BY day) AS cumulative_fc_owners,
    combined_owners,
    SUM(combined_owners) OVER (ORDER BY day) AS cumulative_combined_owners,
    AVG(combined_owners) OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS avg_30day_combined_owners,
    AVG(fc_owners) OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS fc_moving_average_30,
    AVG(ud_owners) OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS ud_moving_average_30,
    AVG(ens_owners) OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS ens_moving_average_30,
    AVG(lens_owners) OVER (ORDER BY day ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS lens_moving_average_30
  FROM owners
)
SELECT *
FROM final
WHERE day >= CAST('2022-11-01' AS TIMESTAMP)
order by
  day