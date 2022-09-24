{{ config(
    materialized='incremental',
    unique_key='external_id',
    incremental_strategy='delete+insert',
) }}


SELECT 
    DISTINCT asana_users._airbyte_asana_users_hashid as external_id,
    NOW() as created,
    NOW() as modified,
    nextval('project_management_projectmanagementproject_id_seq'::regclass) as id,
    'asana' as source,
    '{}'::jsonb as last_raw_data, 
    asana_users.name as name,
    asana_users.email as email,
    NULL as url,
    NULL as status,
    NULL as firstname,
    NULL as lastname,
    NULL as title,
    NULL as roles,
    NULL as company_name,
    NULL as phone,
    NULL as timezone,
    TRUE as active,
    asana_users_photo.image_128x128 as avatar,
    NULL::uuid as integration_id
FROM asana_users
LEFT JOIN asana_users_photo ON asana_users_photo._airbyte_asana_users_hashid = asana_users._airbyte_asana_users_hashid

