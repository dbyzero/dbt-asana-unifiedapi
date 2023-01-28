{{ config(
    materialized='incremental',
    unique_key='external_id',
    incremental_strategy='delete+insert',
) }}

SELECT 
    DISTINCT "{{ var("table_prefix") }}_users".gid as external_id,
    NOW() as created,
    NOW() as modified,
    '{{ var("timestamp") }}' as sync_timestamp,
    md5(
      '{{ var("integration_id") }}' ||
      "{{ var("table_prefix") }}_users".gid ||
      'user' ||
      'asana'
    )  as id,
    'asana' as source,
    _airbyte_raw_{{ var("table_prefix") }}_users._airbyte_data as last_raw_data, 
    "{{ var("table_prefix") }}_users".name as name,
    "{{ var("table_prefix") }}_users".email as email,
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
    "{{ var("table_prefix") }}_users_photo".image_128x128 as avatar,
    '{{ var("integration_id") }}'::uuid  as integration_id
FROM "{{ var("table_prefix") }}_users"
LEFT JOIN "{{ var("table_prefix") }}_users_photo" ON "{{ var("table_prefix") }}_users_photo"._airbyte_{{ var("table_prefix") }}_users_hashid = "{{ var("table_prefix") }}_users"._airbyte_{{ var("table_prefix") }}_users_hashid
LEFT JOIN _airbyte_raw_{{ var("table_prefix") }}_users ON _airbyte_raw_{{ var("table_prefix") }}_users._airbyte_ab_id = "{{ var("table_prefix") }}_users"._airbyte_ab_id
WHERE workspaces @> CONCAT('[{"gid": "', '{{ var("workspace_id") }}', '"}]')::jsonb
