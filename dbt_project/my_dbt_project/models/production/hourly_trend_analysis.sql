-- Hourly trend analysis
{{ config(materialized="table", unique_key=["activity_hour", "event_type"]) }}

select
    date_trunc("hour", event_created_at) as activity_hour,
    event_type,
    count(*) as event_count,
    count(distinct actor_login) as unique_users,
    count(distinct repo_name) as unique_repos,
    extract(hour from event_created_at) as hour_of_day,
    extract(dow from event_created_at) as day_of_week
from {{ ref("stg_github_events") }}
group by date_trunc("hour", event_created_at), event_type, extract(hour from event_created_at), extract(dow from event_created_at)
order by activity_hour desc, event_count desc
