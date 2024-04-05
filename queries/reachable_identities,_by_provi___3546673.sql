-- part of a query repo
-- query name: Reachable identities, by provider
-- query link: https://dune.com/queries/3546673


WITH xmtp AS (
  SELECT
    FROM_HEX(REPLACE(wallet_address, '/', '')) AS wallet_address
  FROM
    dune.xmtp_team.dataset_xmtp_activated_addresses
  WHERE
    LENGTH(REPLACE(wallet_address, '/', '')) = 42
),
ens_identities AS (
  SELECT
    name AS ens
  FROM
    ens.view_registrations
  WHERE
    owner IN (SELECT wallet_address FROM xmtp)
),
lens_identities AS (
  SELECT
    tokenId AS lens
  FROM
    lens_polygon.LensHub_evt_Transfer
  WHERE
    "to" IN (SELECT wallet_address FROM xmtp)
),
ud_identities AS (
  SELECT
    tokenId AS ud
  FROM
    unstoppabledomains_ethereum.UNSRegistry_evt_Transfer
  WHERE
    "to" IN (SELECT wallet_address FROM xmtp)
  UNION
  SELECT
    tokenId AS ud
  FROM
    unstoppabledomains_polygon.UNSRegistry_evt_Transfer
  WHERE
    "to" IN (SELECT wallet_address FROM xmtp)
),
fc_identities AS (
  SELECT
    fid AS fc
  FROM
    dune.neynar.dataset_farcaster_verifications
  WHERE
    FROM_HEX(TRY_CAST(JSON_EXTRACT(claim, '$.address') AS VARCHAR)) IN (SELECT wallet_address FROM xmtp)
)
SELECT
  'ENS' AS provider,
  COUNT(DISTINCT ens) AS identities
FROM
  ens_identities
UNION
SELECT
  'Lens' AS provider,
  COUNT(DISTINCT lens) AS identities
FROM
  lens_identities
UNION
SELECT
  'Unstoppable' AS provider,
  COUNT(DISTINCT ud) AS identities
FROM
  ud_identities
UNION
SELECT
  'Farcaster' AS provider,
  COUNT(DISTINCT fc) AS identities
FROM
  fc_identities
UNION
SELECT
  'Coinbase' AS provider,
  identities
FROM
  dune.xmtp_team.dataset_xmtp_cbid_total_identities_03_18_24
    --   Coinbase cb.id offchain resolver is indexed by Airstack and currently unavailable on Dune.