create or replace table customer_attribution_channel as (
with all_formatted_data as (
select a.event_name, a.event_id, a.client_id, a.page_url, a.timestamp, a.session_id, a.traffic_channel, a.first_traffic_channel_last_7d, a.latest_traffic_channel_last_7d,
case when event_name = 'checkout_completed' then true else false end as is_conversion_event
from enrich_url_params_and_7d_attribution a)

select distinct client_id, is_conversion_event, first_traffic_channel_last_7d, latest_traffic_channel_last_7d
from all_formatted_data
where is_conversion_event

)
