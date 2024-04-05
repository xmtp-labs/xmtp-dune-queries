-- part of a query repo
-- query name: XMTP total activated wallets
-- query link: https://dune.com/queries/3546875


SELECT
  COUNT(DISTINCT wallet_address) AS activations
FROM dune.xmtp_team.dataset_xmtp_activated_addresses