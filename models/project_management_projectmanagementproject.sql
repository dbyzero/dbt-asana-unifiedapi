{{ config(
    materialized='incremental',
    unique_key='external_id',
    incremental_strategy='delete+insert',
) }}

SELECT 
    DISTINCT asana_projects._airbyte_asana_projects_hashid as external_id,
    NOW() as created,
    NOW() as modified,
    nextval('project_management_projectmanagementproject_id_seq'::regclass) as id,
    'asana' as source,
    asana_projects.name as name,
    asana_projects_team.name as folder,
    asana_projects.permalink_url as url,
    NULL as status,
    NULL::boolean as private,
    NULL as description,
    NULL::date as creation_date,
    NULL::date as begin_date,
    NULL::date as end_date,
    NULL::int as owner_id, 
    NULL::uuid as integration_id,
    '{}'::jsonb as last_raw_data 
FROM asana_projects
    left join asana_projects_team on asana_projects.team->>'gid' = asana_projects_team.gid 
WHERE asana_projects.resource_type = 'project'