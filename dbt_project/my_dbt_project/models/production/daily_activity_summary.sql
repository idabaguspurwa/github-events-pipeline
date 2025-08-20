-- Daily activity summary
{{ config(materialized="table", unique_key="activity_date") }}

select
    date(event_created_at) as activity_date,
    count(*) as total_events,
    count(distinct actor_login) as unique_users,
    count(distinct repo_name) as unique_repositories,
    count(case when event_type = 'PushEvent' then 1 end) as push_events,
    count(case when event_type = 'PullRequestEvent' then 1 end) as pull_request_events,
    count(case when event_type = 'IssuesEvent' then 1 end) as issue_events,
    count(case when event_type = 'CreateEvent' then 1 end) as create_events,
    max(event_created_at) as latest_activity
from {{ ref("stg_github_events") }}
group by date(event_created_at)
order by activity_date desc
