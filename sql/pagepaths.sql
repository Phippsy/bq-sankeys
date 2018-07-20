SELECT *,
	LEAD(timestamp,1) OVER (PARTITION BY fullVisitorId, visitID order by timestamp) - timestamp  AS page_duration,
	LEAD(pagePath,1) OVER (PARTITION BY fullVisitorId, visitID order by timestamp) AS next_page,
	TIMESTAMP_SECONDS(CAST(timestamp AS INT64)) visit_timestamp,
	RANK() OVER (PARTITION BY fullVisitorId, visitID order by timestamp) AS step_number
	FROM(
	SELECT   
		pages.fullVisitorID,
		pages.visitID,
		pages.visitNumber,
		pages.pagePath,
		visitors.campaign,
		MIN(pages.timestamp) timestamp

	FROM (
		SELECT
		fullVisitorId,
		visitId,
		trafficSource.campaign campaign
		FROM
		`bigquery-public-data.google_analytics_sample.ga_sessions_*`,
		UNNEST(hits) as hits
		WHERE
		_TABLE_SUFFIX BETWEEN '{{date_from}}' AND '{{date_to}}'
		AND
		hits.type='PAGE'
		
		{{page_filter}}
		{{campaign_filter}}
		{{medium_filter}}
		) AS visitors
		
	JOIN(
		SELECT
		fullVisitorId,
		visitId,
		visitNumber,
		visitStartTime + hits.time/1000 AS TimeStamp,
		hits.page.pagePath AS pagePath
		FROM
		`bigquery-public-data.google_analytics_sample.ga_sessions_*`,
		UNNEST(hits) as hits
		WHERE
		_TABLE_SUFFIX BETWEEN '{{date_from}}' AND '{{date_to}}' ) as pages

	ON
	  visitors.fullVisitorID = pages.fullVisitorID
	  AND visitors.visitID = pages.visitID

	GROUP BY 
	pages.fullVisitorID, visitors.campaign, pages.visitID, pages.visitNumber, pages.pagePath
	ORDER BY 
	pages.fullVisitorID, pages.visitID, pages.visitNumber, timestamp)
	ORDER BY fullVisitorId, step_number