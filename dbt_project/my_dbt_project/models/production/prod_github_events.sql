-- models/production/prod_github_events.sql
{{
  config(
    cluster_by = ['event_created_at']
  )
}}

select *
from {{ ref('stg_github_events') }}
where event_type is not null