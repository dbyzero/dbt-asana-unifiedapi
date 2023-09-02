{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='delete+insert',
)}}

SELECT
    md5(
        '{{ var("integration_id") }}'::text ||
        project.id::text ||
        "{{ var("table_prefix") }}_sections"."gid"::text ||
        'asana'::text
    ) as id,
    "{{ var("table_prefix") }}_sections"."gid" as external_id,
    'asana' as source,
    NOW() as created,
    NOW() as modified,
    '{{ var("integration_id") }}'::uuid as integration_id,
    _airbyte_raw_{{ var("table_prefix") }}_sections._airbyte_data as last_raw_data, 
    NULL as url,
    {{ var("table_prefix") }}_sections.name,
    0::boolean as deleted,
    0::boolean as archived,
    project.id as project_id,
    '{{ var("timestamp") }}' as sync_timestamp
FROM "{{ var("table_prefix") }}_sections"
    LEFT JOIN {{ ref('project_management_projectmanagementproject') }} as project
        on "{{ var("table_prefix") }}_sections".project->>'gid' = project.external_id
        and project.source = 'asana'
        and project.integration_id = '{{ var("integration_id") }}'
    left join _airbyte_raw_{{ var("table_prefix") }}_sections
        on _airbyte_raw_{{ var("table_prefix") }}_sections._airbyte_ab_id = "{{ var("table_prefix") }}_sections"._airbyte_ab_id
WHERE project.id IS NOT NULL