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
    project_management_projectmanagementissuetype.id as issue_type_id,
    project_management_projectmanagementissuetype.project_id as project_id,
    project_management_projectmanagementissuetype.integration_id as integration_id
FROM {{ ref('project_management_projectmanagementissuetype') }} cross join (
    SELECT  
        NOW() as created,
        NOW() as modified,
        'asana' as source,
        '{}'::jsonb as last_raw_data, 
        types.key as external_id,
        types.name as name,
        NULL as description,
        types.type as type,
        NULL as path
    FROM (
        (SELECT 'status' as key, 'status' as name, 'string' as type) UNION
        (SELECT 'name' as key, 'name' as name, 'string' as type)
    ) as types
) as type_list where integration_id = '{{ var("integration_id") }}'