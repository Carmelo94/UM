SELECT
	DATEADD(DAY, 1-DATEPART(WEEKDAY, DATEADD(DAY,-1, full_date)),DATEADD(DAY,-1, full_date)) "week",
	campaign_name,
	campaign_id,
	placement_name,
	placement_id,
	site_name,
	site_id,
	source,
	metric,
	SUM(value) AS "value"
FROM schema.table_name
WHERE
	(full_date >= 'load_start_date' AND full_date <= 'load_end_date') AND
	source IN ('dcm', 'override', 'cadreon') AND
	campaign_name LIKE 'US%' AND
	campaign_id IN ('load_campaign_id') AND
	placement_id IN ('load_placement_id') AND
	metric IN ('load_metrics')
GROUP BY
	--full_date,
	DATEADD(DAY, 1-DATEPART(WEEKDAY, DATEADD(DAY,-1, full_date)),DATEADD(DAY,-1, full_date)),
	campaign_name,
	campaign_id,
	placement_name,
	placement_id,
	site_name,
	site_id,
	source,
	metric