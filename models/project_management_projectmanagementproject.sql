{{ config(
    materialized='incremental',
    unique_key='external_id',
    incremental_strategy='delete+insert',
) }}

SELECT 
    DISTINCT {{ var("table_prefix") }}_projects.gid as external_id,
    NOW() as created,
    NOW() as modified,
    '{{ var("timestamp") }}' as sync_timestamp,
    md5(
      {{ var("table_prefix") }}_projects.gid ||
      'project' ||
      'asana' ||
      '{{ var("integration_id") }}'
    )  as id,
    'asana' as source,
    {{ var("table_prefix") }}_projects.name as name,
    {{ var("table_prefix") }}_projects_team.name as folder,
    {{ var("table_prefix") }}_projects.permalink_url as url,
    NULL as status,
    {{ var("table_prefix") }}_projects.public IS false as private,
    NULL as description,
    NULL::date as creation_date,
    NULL::date as begin_date,
    NULL::date as end_date,
    owner.id as owner_id, 
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_projects._airbyte_data as last_raw_data 
FROM {{ var("table_prefix") }}_projects
    left join {{ var("table_prefix") }}_projects_team
        on {{ var("table_prefix") }}_projects.team->>'gid' = {{ var("table_prefix") }}_projects_team.gid 
    left join {{ ref('project_management_projectmanagementuser') }} as owner
        on owner.external_id = {{ var("table_prefix") }}_projects.owner->>'gid' and owner.source = 'asana' and owner.integration_id = '{{ var("integration_id") }}'
    left join _airbyte_raw_{{ var("table_prefix") }}_projects
        on _airbyte_raw_{{ var("table_prefix") }}_projects._airbyte_ab_id = {{ var("table_prefix") }}_projects._airbyte_ab_id
WHERE {{ var("table_prefix") }}_projects.resource_type = 'project'
AND {{ var("table_prefix") }}_projects.workspace->>'gid' = '{{ var("workspace_id") }}'