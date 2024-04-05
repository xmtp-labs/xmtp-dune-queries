-- part of a query repo
-- query name: XMTP daily identity activations, by provider (ex-CB)
-- query link: https://dune.com/queries/3589083


WITH xmtp AS (
  SELECT
    timestamp,
    FROM_HEX(REPLACE(wallet_address, '/')) AS wallet_address
  FROM
    dune.xmtp_team.dataset_xmtp_activated_addresses
  WHERE
    LENGTH(REPLACE(wallet_address, '/')) = 42
),
ens_identities AS (
  SELECT
    owner AS wallet_address,
    name AS ens
  FROM
    ens.view_registrations
  WHERE
    owner IN (SELECT wallet_address FROM xmtp)
),
lens_identities AS (
  SELECT
    "to" AS wallet_address,
    tokenId AS lens
  FROM
    lens_polygon.LensHub_evt_Transfer
  WHERE
    "to" IN (SELECT wallet_address FROM xmtp)
),
ud_identities AS (
  SELECT
    "to" AS wallet_address,
    tokenId AS ud
  FROM
    unstoppabledomains_ethereum.UNSRegistry_evt_Transfer
  WHERE
    "to" IN (SELECT wallet_address FROM xmtp)
  UNION ALL
  SELECT
    "to" AS wallet_address,
    tokenId AS ud
  FROM
    unstoppabledomains_polygon.UNSRegistry_evt_Transfer
  WHERE
    "to" IN (SELECT wallet_address FROM xmtp)
),
fc_identities AS (
  SELECT
    fid AS fc,
    FROM_HEX(
      TRY_CAST(JSON_EXTRACT(claim, '$.address') AS VARCHAR)
    ) AS wallet_address
  FROM
    dune.neynar.dataset_farcaster_verifications
  WHERE
    FROM_HEX(
      TRY_CAST(JSON_EXTRACT(claim, '$.address') AS VARCHAR)
    ) IN (SELECT wallet_address FROM xmtp)
)
SELECT
  day,
  fc,
  ud,
  lens,
  ens,
  SUM(fc) OVER (ORDER BY day) AS sum_fc,
  SUM(ud) OVER (ORDER BY day) AS sum_ud,
  SUM(lens) OVER (ORDER BY day) AS sum_lens,
  SUM(ens) OVER (ORDER BY day) AS sum_ens,
  SUM(fc + ud + lens + ens) OVER (ORDER BY day) AS sum_total,
  AVG(fc) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS fc_ma_7,
  AVG(ud) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ud_ma_7,
  AVG(lens) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS lens_ma_7,
  AVG(ens) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ens_ma_7
FROM (
  SELECT
    DATE_TRUNC('day', xmtp.timestamp) AS day,
    COUNT(DISTINCT fc_identities.fc) AS fc,
    COUNT(DISTINCT ud_identities.ud) AS ud,
    COUNT(DISTINCT lens_identities.lens) AS lens,
    COUNT(DISTINCT ens_identities.ens) AS ens
  FROM
    xmtp
    LEFT JOIN fc_identities ON xmtp.wallet_address = fc_identities.wallet_address
    LEFT JOIN ud_identities ON xmtp.wallet_address = ud_identities.wallet_address
    LEFT JOIN lens_identities ON xmtp.wallet_address = lens_identities.wallet_address
    LEFT JOIN ens_identities ON xmtp.wallet_address = ens_identities.wallet_address
  GROUP BY
    DATE_TRUNC('day', xmtp.timestamp)
) aggregated_data
WHERE
  day >= DATE_ADD('day', -30, CURRENT_DATE)
ORDER BY
  day;