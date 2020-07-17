update mrr_merchants.manage_sources
set from_ts = to_ts, to_ts = '{to_ts}'
where table_name = '{tableName}';