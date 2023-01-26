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
      '{{ var("integration_id") }}' ||
      project.id ||
      asana_tasks.gid ||
      'task' ||
      'asana'
    )  as id,
    'asana' as source,
    '{{ var("integration_id") }}'::uuid as integration_id,
    '{{ var("timestamp") }}' as sync_timestamp,
    _airbyte_raw_asana_tasks._airbyte_data as last_raw_data, 
    asana_tasks.permalink_url as url,
    NULL as priority,
    NULL as severity,
    asana_tasks.name,
    asana_tasks.notes as description,
    asana_tasks.due_on::date as due_date,
    asana_tasks.completed_at IS NOT NULL as complete,
    NULL as tags,
    assignee.id as assignee_id,
    NULL as creator_id,
    project.id as project_id,
    status.id as status_id,
    type.id as type_id,
    asana_tasks.resource_subtype = 'milestone' as is_milestone
FROM asana_tasks
	LEFT JOIN (
		SELECT jsonb_array_elements(asana_tasks.projects)->>'gid' as groupid , gid FROM asana_tasks
	) as projects on projects.gid = asana_tasks.gid
    LEFT JOIN {{ ref('project_management_projectmanagementproject') }} as project
        on projects.groupid = project.external_id
        and project.source = 'asana'
        and project.integration_id = '{{ var("integration_id") }}'
    LEFT JOIN {{ ref('project_management_projectmanagementissuetype') }} AS type 
        ON type.external_id = 'task'
        AND type.integration_id = '{{ var("integration_id") }}'
        AND type.project_id = project.id
    LEFT JOIN {{ ref('project_management_projectmanagementissuestatus') }} AS status
        ON status.external_id = (CASE WHEN asana_tasks.completed_at IS NOT NULL THEN 'completed' ELSE 'todo' END )
        AND status.integration_id = '{{ var("integration_id") }}'
        AND status.project_id = project.id
    left join {{ ref('project_management_projectmanagementuser') }} as assignee
        on asana_tasks.assignee->>'gid' = assignee.external_id
        and assignee.source = 'asana'
        and assignee.integration_id = '{{ var("integration_id") }}'
    left join _airbyte_raw_asana_tasks
        on _airbyte_raw_asana_tasks._airbyte_ab_id = asana_tasks._airbyte_ab_id
WHERE (
    asana_tasks.is_rendered_as_separator = false OR
    asana_tasks.is_rendered_as_separator IS NULL
)
AND asana_tasks.workspace->>'gid' = '{{ var("workspace_id") }}'
