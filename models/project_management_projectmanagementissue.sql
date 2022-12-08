{{ config(
    materialized='incremental',
    unique_key='external_id',
    incremental_strategy='delete+insert',
)}}

SELECT 
    DISTINCT asana_tasks.gid as external_id,
    NOW() as created,
    NOW() as modified,
    md5(
      asana_tasks.gid ||
      'task' ||
      'asana'
    )  as id,
    'asana' as source,
    '{{ var("integration_id") }}'::uuid as integration_id,
    _airbyte_raw_asana_tasks._airbyte_data as last_raw_data, 
    asana_tasks.permalink_url as url,
    NULL as priority,
    NULL as severity,
    asana_tasks.name,
    asana_tasks.notes as description,
    NULL::date as due_date,
    asana_tasks.completed_at IS NOT NULL as complete,
    NULL as tags,
    assignee.id as assignee_id,
    NULL as creator_id,
    project.id as project_id,
    NULL as status_id,
    -- asana_tasks.resource_type as type,
    NULL as type_id
FROM asana_tasks
    left join {{ ref('project_management_projectmanagementproject') }} as project
        on asana_tasks.projects->0->>'gid' = project.external_id and project.source = 'asana' 
    left join {{ ref('project_management_projectmanagementuser') }} as assignee
        on asana_tasks.assignee->>'gid' = assignee.external_id and assignee.source = 'asana'
    left join _airbyte_raw_asana_tasks
        on _airbyte_raw_asana_tasks._airbyte_ab_id = asana_tasks._airbyte_ab_id
WHERE asana_tasks.is_rendered_as_separator = false
