SELECT
	full_date,
	--DATEADD(DAY, 1-DATEPART(WEEKDAY, DATEADD(DAY,-1, full_date)),DATEADD(DAY,-1, full_date)) "week",
	universal_campaign,
	universal_adset,
	universal_campaign_id,
	universal_adset_id,
	universal_metric,
	sum(value) AS "value"
FROM amex.v_social_joined_updated
WHERE
	(full_date >= 'load_start_date' AND full_date <=  'load_end_date') AND
	universal_metric IN ('load_metrics') AND
	universal_campaign IN ('load_campaign_name') 
GROUP BY
	full_date,
	--DATEADD(DAY, 1-DATEPART(WEEKDAY, DATEADD(DAY,-1, full_date)),DATEADD(DAY,-1, full_date)),
	universal_campaign,
	universal_adset,
	universal_campaign_id,
	universal_adset_id,
	universal_metric
