{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
)}}

SELECT 
    DISTINCT "{{ var("table_prefix") }}_tasks".gid as external_id,
    NOW() as created,
    NOW() as modified,
    md5(
      '{{ var("integration_id") }}' ||
      project.id ||
      "{{ var("table_prefix") }}_tasks".gid ||
      'task' ||
      'asana'
    )  as id,
    'asana' as source,
    '{{ var("integration_id") }}'::uuid as integration_id,
    '{{ var("timestamp") }}' as sync_timestamp,
    _airbyte_raw_{{ var("table_prefix") }}_tasks._airbyte_data as last_raw_data, 
    "{{ var("table_prefix") }}_tasks".permalink_url as url,
    NULL as priority,
    NULL as severity,
    "{{ var("table_prefix") }}_tasks".name,
    "{{ var("table_prefix") }}_tasks".notes as description,
    "{{ var("table_prefix") }}_tasks".due_on::date as due_date,
    "{{ var("table_prefix") }}_tasks".completed_at IS NOT NULL as complete,
    NULL as tags,
    _group.id as group_id,
    assignee.id as assignee_id,
    NULL as creator_id,
    project.id as project_id,
    status.id as status_id,
    type.id as type_id,
    "{{ var("table_prefix") }}_tasks".resource_subtype = 'milestone' as is_milestone
FROM "{{ var("table_prefix") }}_tasks"
	LEFT JOIN (
		SELECT jsonb_array_elements("{{ var("table_prefix") }}_tasks".projects)->>'gid' as groupid , gid FROM "{{ var("table_prefix") }}_tasks"
	) as projects on projects.gid = "{{ var("table_prefix") }}_tasks".gid
    left join "{{ var("table_prefix") }}_tasks_memberships" as section
        on section."_airbyte_ab_id" = "{{var("table_prefix")}}_tasks"."_airbyte_ab_id"
    left join {{ ref('project_management_projectmanagementgroup')}} as _group
        on _group.external_id = section.section->>'gid'
    LEFT JOIN {{ ref('project_management_projectmanagementproject') }} as project
        on projects.groupid = project.external_id
        and project.source = 'asana'
        and project.integration_id = '{{ var("integration_id") }}'
    LEFT JOIN {{ ref('project_management_projectmanagementissuetype') }} AS type 
        ON type.external_id = 'task'
        AND type.integration_id = '{{ var("integration_id") }}'
        AND type.project_id = project.id
    LEFT JOIN {{ ref('project_management_projectmanagementissuestatus') }} AS status
        ON status.external_id = (CASE WHEN "{{ var("table_prefix") }}_tasks".completed_at IS NOT NULL THEN 'completed' ELSE 'todo' END )
        AND status.integration_id = '{{ var("integration_id") }}'
        AND status.project_id = project.id
    left join {{ ref('project_management_projectmanagementuser') }} as assignee
        on "{{ var("table_prefix") }}_tasks".assignee->>'gid' = assignee.external_id
        and assignee.source = 'asana'
        and assignee.integration_id = '{{ var("integration_id") }}'
    left join _airbyte_raw_{{ var("table_prefix") }}_tasks
        on _airbyte_raw_{{ var("table_prefix") }}_tasks._airbyte_ab_id = "{{ var("table_prefix") }}_tasks"._airbyte_ab_id
WHERE (
    "{{ var("table_prefix") }}_tasks".is_rendered_as_separator = false OR
    "{{ var("table_prefix") }}_tasks".is_rendered_as_separator IS NULL
)
AND "{{ var("table_prefix") }}_tasks".workspace->>'gid' = '{{ var("workspace_id") }}'
