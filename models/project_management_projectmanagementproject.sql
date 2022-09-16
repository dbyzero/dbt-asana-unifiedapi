{{ config(
    materialized='incremental',
    unique_key='external_id'
) }}

SELECT 
    DISTINCT asana_projects.gid as external_id,
    NOW() as created,
    NOW() as modified,
    UUID('e3566198-fbae-4f67-98c4-7868350ce742') as id,
    'asana' as source,
    {} as last_raw_data, 
    asana_projects.name as name,
    asana_projects_team.name as folder,
    asana_projects.permalink_url as url,
    NULL as status,
    NULL as private,
    NULL as description,
    NULL as creation_date,
    NULL as begin_date,
    NULL as end_date,
    NULL as owner_id, 
    NULL as integration_id
FROM asana_projects
    left join asana_projects_team on asana_projects.team->>'gid' = asana_projects_team.gid 
    left join asana_users on asana_projects.owner->>'gid' = asana_users.gid 
WHERE asana_projects.resource_type = 'project'
