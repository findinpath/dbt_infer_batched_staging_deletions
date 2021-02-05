{{ config(materialized = 'table') }}


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
    export_time,
    product_id,
    NULL                                              AS product_name,
    file                                              AS file,
    TRUE                                              AS _deleted
FROM (
        SELECT product_id,
               export_time,
               file
        FROM (
                -- Obtain the export_time and previous export_time for a product
                -- in order to be able to filter out the eventual duplicated entries
                -- Once an entry is deactivated, it is quite likely that it will not
                -- appear anymore in the following staging entries.
                SELECT product_id,
                       export_time,
                       file,
                       LAG(export_time) OVER (PARTITION BY product_id ORDER BY export_time) prev_export_time
                FROM (
                          -- retrieve the products which don't appear on a the staged date.
                          -- NOTE that here is made the the supposition that all the entries staged in
                          -- one day share the same `export_time`. This is why the cardinality of `staged_date.export_time`
                          -- should remain relatively small.
                          SELECT product_id,
                                 export_time,
                                 file
                          FROM (
                                  (SELECT DISTINCT product_id
                                  FROM {{ source('dbt_shop', 'raw_products') }})
                                  CROSS JOIN
                                  (SELECT DISTINCT export_time, file
                                  FROM {{ source('dbt_shop', 'raw_products') }}) AS staged_date
                          )
                ) staged_product_date
                {#
                   Take into account only the entries that exist
                   after the first staging appearance of the product
                #}
                WHERE staged_product_date.export_time > (
                                                             SELECT MIN(export_time)
                                                             FROM {{ source('dbt_shop', 'raw_products') }}
                                                             WHERE product_id = staged_product_date.product_id
                                                         )
                  AND staged_product_date.export_time
                        NOT IN (
                                    SELECT DISTINCT export_time
                                    FROM {{ source('dbt_shop', 'raw_products') }}
                                    WHERE product_id = staged_product_date.product_id
                               )
        ) AS missing_staged_product
        WHERE (
                -- Either the previous entry does not exist which means we are dealing with the
                -- beginning of the deactivation sequence
                -- or the previous entry is much more in the past meaning that in the meantime
                -- the product has been reactivated and then again deactivated.
                (missing_staged_product.prev_export_time IS NULL)
                OR missing_staged_product.export_time != (
                                                                SELECT MIN(export_time)
                                                                FROM {{ source('dbt_shop', 'raw_products') }}
                                                                WHERE export_time > missing_staged_product.prev_export_time
                                                          )
        )
) AS deduplicated_missing_staged_product