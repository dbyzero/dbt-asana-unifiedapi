{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
)}}

SELECT
    md5(
        project_management_projectmanagementproject.id ||
        project_management_projectmanagementproject.integration_id ||
        status_list.external_id ||
        'issuestatusasana'
    ) as id,
    NULL as meta_status,
    status_list.*,
    project_management_projectmanagementproject.id as project_id,
    project_management_projectmanagementproject.integration_id as integration_id
FROM {{ ref('project_management_projectmanagementproject') }} cross join (
    SELECT  
        NOW() as created,
        NOW() as modified,
        '0'::boolean as default,
        status.color as color,
        status.order as order,
        status.key as external_id,
        status.name as name,
        'asana' as source,
        '{}'::jsonb as last_raw_data
    FROM (
        (SELECT 'completed' as key, 'Completed' as name, 0 as order, 'green' as color) UNION
        (SELECT 'todo' as key, 'Todo' as name, 1 as order, 'orange' as color)
    ) as status
) as status_list where integration_id = '{{ var("integration_id") }}'