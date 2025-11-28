WITH kv AS (
    SELECT
        event_id,
        page_url,
        SPLIT_PART(pair, '=', 1) AS key,
        SPLIT_PART(pair, '=', 2) AS value
    FROM (
        SELECT
            event_id, page_url,
            UNNEST(STRING_SPLIT(SPLIT_PART(page_url, '?', 2), '&')) AS pair
        FROM raw_data_with_session_and_event_id
    )
),
events_with_extracted_url_params as(
SELECT
    event_id,
    page_url,
    MAX(CASE WHEN key = '_kx' THEN value END) AS _kx,
    MAX(CASE WHEN key = 'bxid' THEN value END) AS bxid,
    MAX(CASE WHEN key = 'locale' THEN value END) AS locale,
    MAX(CASE WHEN key = 'utm_campaign' THEN value END) AS utm_campaign,
    MAX(CASE WHEN key = 'utm_content' THEN value END) AS utm_content,
    MAX(CASE WHEN key = 'utm_medium' THEN value END) AS utm_medium,
    MAX(CASE WHEN key = 'utm_source' THEN value END) AS utm_source,
    MAX(CASE WHEN key = 'gclid' then value END) as gclid,
    MAX(CASE WHEN key = 'fbclid' then value END) as fbclid,
    MAX(CASE WHEN key = 'irclickid' then value END) as irclickid,
    MAX(CASE WHEN key = 'msclkid' then value END) as msclkid
    
FROM kv
GROUP BY 1,2)

select a.*, utm_campaign, utm_content, utm_medium, utm_source, gclid, fbclid, irclickid, msclkid
 from raw_data_with_session_and_event_id a
left join events_with_extracted_url_params b on a.event_id = b.event_id
;
