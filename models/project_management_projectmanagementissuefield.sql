{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
)}}

SELECT
    md5(
        project_management_projectmanagementissuetype.integration_id ||
        project_management_projectmanagementissuetype.project_id ||
        project_management_projectmanagementissuetype.id ||
        type_list.external_id ||
        'issuefieldasana'
    ) as id,
    type_list.*,
    '{{ var("timestamp") }}' as sync_timestamp,
    project_management_projectmanagementissuetype.id as issue_type_id,
    project_management_projectmanagementissuetype.project_id as project_id,
    project_management_projectmanagementissuetype.integration_id as integration_id
FROM {{ ref('project_management_projectmanagementissuetype') }} cross join (
    SELECT  
        types.key as external_id,
        NOW() as created,
        NOW() as modified,
        'asana' as source,
        '{}'::jsonb as last_raw_data, 
        types.name as name,
        NULL as description,
        types.type as type,
        types.key as path
    FROM (
        (SELECT 'gid' as key, 'gid' as name, 'string' as type) UNION
        (SELECT 'resource_type' as key, 'Resource Type' as name, 'string' as type) UNION
        (SELECT 'approval_status' as key, 'Approval Status' as name, 'string' as type) UNION
        (SELECT 'assignee_status' as key, 'Assignee Status' as name, 'string' as type) UNION
        (SELECT 'complete' as key, 'Complete' as name, 'boolean' as type) UNION
        (SELECT 'complete_at' as key, 'Complete At' as name, 'timestamp' as type) UNION
        (SELECT 'complete_by' as key, 'Complete By' as name, 'object' as type) UNION
        (SELECT 'created_at' as key, 'Created At' as name, 'timestamp' as type) UNION
        (SELECT 'dependencies' as key, 'Dependencies' as name, 'array' as type) UNION
        (SELECT 'dependents' as key, 'Dependents' as name, 'array' as type) UNION
        (SELECT 'due_at' as key, 'Due At' as name, 'timestamp' as type) UNION
        (SELECT 'due_on' as key, 'Due On' as name, 'date' as type) UNION
        (SELECT 'external' as key, 'External' as name, 'object' as type) UNION
        (SELECT 'hearted' as key, 'Hearted' as name, 'boolean' as type) UNION
        (SELECT 'hearts' as key, 'Hearts' as name, 'array' as type) UNION
        (SELECT 'html_notes' as key, 'HTML Note' as name, 'string' as type) UNION
        (SELECT 'liked' as key, 'Liked' as name, 'boolean' as type) UNION
        (SELECT 'likes' as key, 'Likes' as name, 'array' as type) UNION
        (SELECT 'memberships' as key, 'Memberships' as name, 'array' as type) UNION
        (SELECT 'modified_at' as key, 'Modified at' as name, 'date' as type) UNION
        (SELECT 'name' as key, 'Name' as name, 'string' as type) UNION
        (SELECT 'notes' as key, 'Notes' as name, 'string' as type) UNION
        (SELECT 'num_hearts' as key, 'Num hearts' as name, 'integer' as type) UNION
        (SELECT 'num_likes' as key, 'Num likes' as name, 'integer' as type) UNION
        (SELECT 'num_subtasks' as key, 'Num subtasks' as name, 'integer' as type) UNION
        (SELECT 'resource_subtype' as key, 'Resource subtype' as name, 'object' as type) UNION
        (SELECT 'start_at' as key, 'Start at' as name, 'timestamp' as type) UNION
        (SELECT 'start_on' as key, 'Start on' as name, 'date' as type) UNION
        (SELECT 'assignee' as key, 'Assignee' as name, 'object' as type) UNION
        (SELECT 'assignee_section' as key, 'Assignee section' as name, 'object' as type) UNION
        (SELECT 'followers' as key, 'Followers' as name, 'array' as type) UNION
        (SELECT 'parent' as key, 'Parent' as name, 'object' as type) UNION
        (SELECT 'permalink_url' as key, 'Permalink url' as name, 'string' as type) UNION
        (SELECT 'projects' as key, 'Projects' as name, 'array' as type) UNION
        (SELECT 'tags' as key, 'Tags' as name, 'array' as type)
    ) as types
) as type_list where integration_id = '{{ var("integration_id") }}'
UNION
SELECT
    md5(
        project_management_projectmanagementissuetype.integration_id ||
        project_management_projectmanagementissuetype.project_id ||
        project_management_projectmanagementissuetype.id ||
        custom_field_per_project.custom_field_id ||
        'issuefieldasana'
    ) as id,
    custom_field_per_project.custom_field_id as external_id,
    NOW() as created,
    NOW() as modified,
    'asana' as source,
    '{}'::jsonb as last_raw_data,
    custom_field_per_project.custom_field_name as external_id,
    NULL as description,
    -- custom_field_per_project.custom_field_type as type,
    'string' as type,
    concat('custom_fields[?(@.gid==', custom_field_id ,')].display_value') as path,
    '{{ var("timestamp") }}' as sync_timestamp,
    project_management_projectmanagementissuetype.id as issue_type_id,
    project_management_projectmanagementissuetype.project_id as project_id,
    project_management_projectmanagementissuetype.integration_id as integration_id
FROM {{ ref('project_management_projectmanagementissuetype') }} left join (
	SELECT 
		distinct 
	    md5(
	      projectid ||
	      'project' ||
	      'asana' ||
          '{{ var("integration_id") }}'
	    )  as projectid,
	    'asana' as source,
		custom_field_id,
		custom_field_name,
		custom_field_type
	FROM (
		SELECT "{{ var("table_prefix") }}_projects".name as projectname,
			atcf.gid as custom_field_id,
			atcf.name as custom_field_name,
			atcf.type as custom_field_type,
			projectid, task_gid,
			tasks._airbyte_{{ var("table_prefix") }}_tasks_hashid
		FROM "{{ var("table_prefix") }}_projects"
		LEFT JOIN
			(SELECT _airbyte_{{ var("table_prefix") }}_tasks_hashid,
				jsonb_array_elements("{{ var("table_prefix") }}_tasks".projects)->>'gid' as projectid ,
				gid as task_gid
			FROM "{{ var("table_prefix") }}_tasks"
			) as tasks
		ON tasks.projectid = "{{ var("table_prefix") }}_projects".gid
		LEFT JOIN "{{ var("table_prefix") }}_tasks_custom_fields" AS atcf
		ON atcf._airbyte_{{ var("table_prefix") }}_tasks_hashid  = tasks._airbyte_{{ var("table_prefix") }}_tasks_hashid
		) AS custom_list
	WHERE custom_field_id IS NOT null
) as custom_field_per_project
ON project_management_projectmanagementissuetype.project_id = custom_field_per_project.projectid
where custom_field_id is not NULL
AND project_id IS NOT NULL
