{{ config(materialized='table') }}

with asana_project as (
    SELECT 
        DISTINCT airbyte_projects.gid as id,
        airbyte_projects.name as name,
        airbyte_projects.permalink_url as url,
        airbyte_projects_team.name as folder,
        airbyte_projects.modified_at as last_modification_date,
        NULL as status,
        NULL as private,
        NULL as description,
        NULL as creation_date,
        NULL as begin_date,
        NULL as end_date,
        airbyte_users.email as owner
    FROM airbyte_projects
    left join airbyte_projects_team on airbyte_projects.team->>'gid' = airbyte_projects_team.gid 
    left join airbyte_users on airbyte_projects.owner->>'gid' = airbyte_users.gid 
    where airbyte_projects.resource_type = 'project'
)

select 
    '{}' as custom_fields,
    FALSE as disabled,
    'asana' as source,
    NULL as last_raw_data, -- TODO
    id as external_id,
    name,
    folder,
    url,
    status,
    private,
    description,
    creation_date,
    begin_date,
    end_date,
    {{ var('customer_id') }} as customer_id,
    NULL as owner_id, -- TODO
    {{ var('workspace_id') }} as workspace_id
from asana_project