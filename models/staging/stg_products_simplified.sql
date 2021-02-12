{{ config(materialized = 'table') }}

WITH export_metadata AS (
    SELECT DISTINCT
        export_time,
        file
    FROM {{ source('dbt_shop', 'raw_products') }}
)

-- create an insert entry for each of the entries present
-- in the raw products staging data.
SELECT
    export_time,
    product_id,
    product_name,
    file,
    -- the entries specified in the source table are definitely not deleted.
    FALSE    AS _deleted
FROM {{ source('dbt_shop', 'raw_products') }}

UNION ALL

-- create a delete entry in case that there are staged entries corresponding
-- that correspond to a product that can be outdated
SELECT
    deletion.export_time,
    deletion.product_id,
    NULL                                   AS product_name,
    export_metadata.file,
    deletion._deleted
FROM (
        SELECT
            next_export_time               AS export_time,
            product_id,
            (
                next_export_time IS NOT NULL
                AND (next_product_export_time IS NULL OR next_product_export_time > next_export_time)
            )                              AS _deleted
        FROM (
                SELECT
                    export_time,
                    LEAD(export_time) OVER
                        (PARTITION BY product_id ORDER BY export_time)                   AS next_product_export_time,
                    (
                        SELECT MIN(export_time)
                        FROM {{ source('dbt_shop', 'raw_products') }}
                        WHERE export_time > src.export_time
                    )                                                                     AS next_export_time,
                    product_id
                    FROM {{ source('dbt_shop', 'raw_products') }} AS src
        )
        WHERE _deleted
) AS deletion
INNER JOIN export_metadata ON deletion.export_time = export_metadata.export_time
