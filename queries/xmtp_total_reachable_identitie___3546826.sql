-- part of a query repo
-- query name: XMTP total reachable identities
-- query link: https://dune.com/queries/3546826


WITH
  xmtp AS (
    SELECT
      FROM_HEX(REPLACE(wallet_address, '/')) as wallet_address
    FROM
      dune.xmtp_team.dataset_xmtp_activated_addresses
    WHERE
      LENGTH(REPLACE(wallet_address, '/')) = 42
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
  ),
  aggregated_identities AS (
    SELECT
      'ENS' as provider,
      count(distinct ens) AS identities
    FROM
      ens_identities
    UNION
    SELECT
      'Lens' as provider,
      count(distinct lens) AS identities
    FROM
      lens_identities
    UNION
    SELECT
      'Unstoppable' as provider,
      count(distinct ud) AS identities
    FROM
      ud_identities
    UNION
    SELECT
      'Farcaster' as provider,
      count(distinct fc) AS identities
    FROM
      fc_identities
    UNION
    SELECT
      'cb' as provider,
      identities
    FROM
      dune.xmtp_team.dataset_xmtp_cbid_total_identities_03_18_24
  )
SELECT
  SUM(identities) AS total_identities
FROM
  aggregated_identities