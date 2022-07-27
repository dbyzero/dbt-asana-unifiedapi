{{ config(
    materialized='incremental',
    unique_key='external_id'
) }}

SELECT 
    NOW() as created,
    NOW() as updated,
    get_random_uuid() as id,
    DISTINCT airbyte_projects.gid as external_id,
    '{}' as custom_fields,
    FALSE as disabled,
    'asana' as source,
    NULL as last_raw_data, -- TODO
    airbyte_projects.name as name,
    airbyte_projects_team.name as folder,
    airbyte_projects.permalink_url as url,
    NULL as status,
    NULL as private,
    NULL as description,
    NULL as creation_date,
    NULL as begin_date,
    NULL as end_date,
    '{{ var("customer_id") }}' as customer_id,
    NULL as owner_id, -- TODO
    '{{ var("workspace_id") }}' as workspace_id
FROM airbyte_projects
    left join airbyte_projects_team on airbyte_projects.team->>'gid' = airbyte_projects_team.gid 
    left join airbyte_users on airbyte_projects.owner->>'gid' = airbyte_users.gid 
WHERE airbyte_projects.resource_type = 'project'