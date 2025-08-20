-- User engagement metrics
{{ config(materialized="table", unique_key="actor_login") }}

select
    actor_login,
    count(*) as total_events,
    count(distinct repo_name) as repositories_contributed,
    count(case when event_type = 'PushEvent' then 1 end) as push_count,
    count(case when event_type = 'PullRequestEvent' then 1 end) as pr_count,
    count(case when event_type = 'IssuesEvent' then 1 end) as issue_count,
    min(event_created_at) as first_activity,
    max(event_created_at) as latest_activity,
    datediff(day, min(event_created_at), max(event_created_at)) + 1 as active_days,
    round(count(*) / nullif(datediff(day, min(event_created_at), max(event_created_at)) + 1, 0), 2) as avg_events_per_day
from {{ ref("stg_github_events") }}
group by actor_login
having count(*) >= 5
order by total_events desc
