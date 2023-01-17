{{ config(
    materialized='incremental',
    unique_key='external_id',
    incremental_strategy='delete+insert',
) }}

SELECT 
    DISTINCT asana_projects.gid as external_id,
    NOW() as created,
    NOW() as modified,
    md5(
      asana_projects.gid ||
      'project' ||
      'asana' ||
      '{{ var("integration_id") }}'
    )  as id,
    'asana' as source,
    asana_projects.name as name,
    asana_projects_team.name as folder,
    asana_projects.permalink_url as url,
    NULL as status,
    asana_projects.public IS false as private,
    NULL as description,
    NULL::date as creation_date,
    NULL::date as begin_date,
    NULL::date as end_date,
    owner.id as owner_id, 
    '{{ var("integration_id") }}'::uuid  as integration_id,
    _airbyte_raw_asana_projects._airbyte_data as last_raw_data 
FROM asana_projects
    left join asana_projects_team
        on asana_projects.team->>'gid' = asana_projects_team.gid 
    left join {{ ref('project_management_projectmanagementuser') }} as owner
        on owner.external_id = asana_projects.owner->>'gid' and owner.source = 'asana' and owner.integration_id = '{{ var("integration_id") }}'
    left join _airbyte_raw_asana_projects
        on _airbyte_raw_asana_projects._airbyte_ab_id = asana_projects._airbyte_ab_id

WHERE asana_projects.resource_type = 'project'
AND asana_projects.workspace->>'gid' = '{{ var("workspace_id") }}'