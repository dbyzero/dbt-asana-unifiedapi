{{ config(
    materialized='incremental',
    unique_key='external_id',
    incremental_strategy='delete+insert',
) }}

SELECT 
    DISTINCT asana_users.gid as external_id,
    NOW() as created,
    NOW() as modified,
    md5(
      '{{ var("integration_id") }}' ||
      asana_users.gid ||
      'user' ||
      'asana'
    )  as id,
    'asana' as source,
    _airbyte_raw_asana_users._airbyte_data as last_raw_data, 
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
    '{{ var("integration_id") }}'::uuid  as integration_id
FROM asana_users
LEFT JOIN asana_users_photo ON asana_users_photo._airbyte_asana_users_hashid = asana_users._airbyte_asana_users_hashid
LEFT JOIN _airbyte_raw_asana_users ON _airbyte_raw_asana_users._airbyte_ab_id = asana_users._airbyte_ab_id

