SELECT
	--full_date,
	DATEADD(DAY, 1-DATEPART(WEEKDAY, DATEADD(DAY,-1, full_date)),DATEADD(DAY,-1, full_date)) "week",
	advertiser,
	engine_account,
	engine_account_type,
	engine_account_id,
	campaign_name,
	campaign_id,
	metric,
	sum(value) as "value"
FROM schema.table_name
WHERE
	(full_date >= 'load_start_date' AND full_date <= 'load_end_date') AND
	campaign_name LIKE 'US%' AND
	advertiser IN ('load_advertisers') AND
	metric IN ('load_metrics')
GROUP BY
	full_date,
	DATEADD(DAY, 1-DATEPART(WEEKDAY, DATEADD(DAY,-1, full_date)),DATEADD(DAY,-1, full_date)),
	advertiser,
	engine_account,
	engine_account_type,
	engine_account_id,
	campaign_name,
	campaign_id,
	metric