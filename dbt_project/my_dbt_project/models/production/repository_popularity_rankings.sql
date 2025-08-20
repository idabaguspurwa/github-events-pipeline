-- Repository popularity rankings
{{ config(materialized="table", unique_key="repo_name") }}

select
    repo_name,
    count(*) as total_activity,
    count(distinct actor_login) as unique_contributors,
    count(case when event_type = 'PushEvent' then 1 end) as push_events,
    count(case when event_type = 'WatchEvent' then 1 end) as watch_events,
    count(case when event_type = 'ForkEvent' then 1 end) as fork_events,
    count(case when event_type = 'PullRequestEvent' then 1 end) as pull_requests,
    count(case when event_type = 'IssuesEvent' then 1 end) as issues,
    max(event_created_at) as latest_activity,
    min(event_created_at) as first_seen,
    round(count(*) / count(distinct date(event_created_at)), 2) as avg_daily_activity
from {{ ref("stg_github_events") }}
group by repo_name
having count(*) >= 10
order by total_activity desc
