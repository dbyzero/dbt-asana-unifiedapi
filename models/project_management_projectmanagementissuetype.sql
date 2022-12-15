{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
)}}

SELECT
    md5(
        project_management_projectmanagementproject.id ||
        project_management_projectmanagementproject.integration_id ||
        type_list.external_id ||
        'issuetypeasana'
    ) as id,
    type_list.*,
    project_management_projectmanagementproject.id as project_id,
    project_management_projectmanagementproject.integration_id as integration_id
FROM {{ ref('project_management_projectmanagementproject') }} cross join (
    SELECT  
        NOW() as created,
        NOW() as modified,
        'asana' as source,
        '{}'::jsonb as last_raw_data, 
        false as is_sub_task,
        types.key as external_id,
        types.name as name,
        NULL as description,
        'project' as scope,
        NULL as url,
        NULL as icon
    FROM (
        (SELECT 'issue' as key, 'Issue' as name)
    ) as types
) as type_list where integration_id = '{{ var("integration_id") }}'


