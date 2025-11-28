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
    -- Existing UTM parameters
MAX(CASE WHEN key = 'utm_campaign' THEN value END) AS utm_campaign,
MAX(CASE WHEN key = 'utm_content' THEN value END) AS utm_content,
MAX(CASE WHEN key = 'utm_medium' THEN value END) AS utm_medium,
MAX(CASE WHEN key = 'utm_source' THEN value END) AS utm_source,

-- Standard UTM/Tracking (Missing)
MAX(CASE WHEN key = 'utm_term' THEN value END) AS utm_term,

-- Google Paid/Tracking IDs
MAX(CASE WHEN key = 'gclid' THEN value END) AS gclid,
MAX(CASE WHEN key = 'gad_source' THEN value END) AS gad_source,
MAX(CASE WHEN key = 'gbraid' THEN value END) AS gbraid,
MAX(CASE WHEN key = '_gl' THEN value END) AS _gl, -- Google Linker

-- Facebook Paid ID
MAX(CASE WHEN key = 'fbclid' THEN value END) AS fbclid,

-- Microsoft/Bing Paid ID
MAX(CASE WHEN key = 'msclkid' THEN value END) AS msclkid,

-- Affiliate/Partner IDs
MAX(CASE WHEN key = 'irclickid' THEN value END) AS irclickid,
MAX(CASE WHEN key = 'irgwc' THEN value END) AS irgwc,
MAX(CASE WHEN key = 'iradid' THEN value END) AS iradid,
MAX(CASE WHEN key = 'im_ref' THEN value END) AS im_ref,
MAX(CASE WHEN key = 'oid' THEN value END) AS oid,
MAX(CASE WHEN key = 'affid' THEN value END) AS affid,
MAX(CASE WHEN key = 'sub1' THEN value END) AS sub1,
MAX(CASE WHEN key = 'sub2' THEN value END) AS sub2,
MAX(CASE WHEN key = 'sub3' THEN value END) AS sub3,
MAX(CASE WHEN key = 'clickid' THEN value END) AS clickid,
MAX(CASE WHEN key = 'sharedid' THEN value END) AS sharedid,
MAX(CASE WHEN key = 'sfdr_ptcid' THEN value END) AS sfdr_ptcid,
MAX(CASE WHEN key = 'sfdr_hash' THEN value END) AS sfdr_hash,

-- Other Tracking IDs
MAX(CASE WHEN key = '_kx' THEN value END) AS _kx,
MAX(CASE WHEN key = 'bxid' THEN value END) AS bxid,
MAX(CASE WHEN key = 'amp_device_id' THEN value END) AS amp_device_id,
MAX(CASE WHEN key = 'locale' THEN value END) AS locale,

CASE
    -- 1. Microsoft/Bing Ads (highest priority for its specificity)
    WHEN msclkid IS NOT NULL THEN 'bing'

    -- 2. Google Ads
    WHEN gclid IS NOT NULL OR gbraid IS NOT NULL OR gad_source IS NOT NULL THEN 'google'

    -- 3. Facebook/Meta Ads
    WHEN fbclid IS NOT NULL THEN 'meta'

    -- 4. Affiliates/Partners (using the high-frequency affiliate parameters)
    WHEN affid IS NOT NULL
      OR oid IS NOT NULL
      OR sub1 IS NOT NULL
      OR irclickid IS NOT NULL
      OR iradid IS NOT NULL
      OR irgwc IS NOT NULL
      OR im_ref IS NOT NULL
      OR sfdr_ptcid IS NOT NULL
        THEN 'affiliates'

      WHEN _kx is not null
      OR bxid is not null 
        THEN 'other - investigate'
      
    -- 5. Organic/Other (Default)
    ELSE 'organic'
END AS traffic_channel
    
FROM kv
GROUP BY 1,2)

select a.*, "_kx", bxid, locale, utm_campaign, utm_content, utm_medium, utm_source, utm_term, gclid, gad_source, gbraid, _gl, fbclid, msclkid, irclickid, irgwc, iradid, im_ref, oid, affid, sub1, sub2, sub3, clickid, sharedid, sfdr_ptcid, sfdr_hash, amp_device_id, traffic_channel
 from raw_data_with_session_and_event_id a
left join events_with_extracted_url_params b on a.event_id = b.event_id
;
