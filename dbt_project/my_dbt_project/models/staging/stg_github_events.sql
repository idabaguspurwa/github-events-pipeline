-- models/staging/stg_github_events.sql
select
    v:id::string as event_id,
    v:type::string as event_type,
    v:actor:id::int as actor_id,
    v:actor:login::string as actor_login,
    v:repo:id::int as repo_id,
    v.repo:name::string as repo_name,
    v:payload:action::string as payload_action,
    v:created_at::timestamp_ntz as event_created_at,
    loaded_at
from {{ source('raw_data', 'raw_events') }}