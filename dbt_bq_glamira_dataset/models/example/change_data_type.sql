{{ config(
    materialized='table',
    schema='your_dataset'
) }}

SELECT
    currency,
    SAFE_CAST(
        REGEXP_REPLACE(
            REPLACE(price, ',', '.'),
            r'(\d)\.(\d{3})(\.\d{2})', '$1$2$3'
        ) AS FLOAT64
    ) AS price,
    _id,
    TIMESTAMP_SECONDS(time_stamp) AS time_stamp,
    SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', local_time) AS local_time,  -- Added SAFE.PARSE for safety
    SAFE_CAST(user_id_db AS INTEGER) AS user_id_db,  -- Convert to INTEGER
    device_id,
    ip,
    user_agent,
    resolution,
    SAFE_CAST(api_version AS FLOAT64) AS api_version,  -- Convert to FLOAT64
    SAFE_CAST(store_id AS INTEGER) AS store_id,  -- Convert to INTEGER
    SAFE_CAST(product_id AS INTEGER) AS product_id,  -- Convert to INTEGER
    SAFE_CAST(order_id AS INTEGER) AS order_id,  -- Convert to INTEGER
    collection,
    email_address,
    current_url,
    referrer_url,
    recommendation,
    utm_source,
    utm_medium,
    show_recommendation,
    is_paypal,  -- No cast needed if already BOOLEAN
    key_search,
    SAFE_CAST(cat_id AS INTEGER) AS cat_id,  -- Convert to INTEGER
    SAFE_CAST(collect_id AS INTEGER) AS collect_id,  -- Convert to INTEGER
    SAFE_CAST(viewing_product_id AS INTEGER) AS viewing_product_id,  -- Convert to INTEGER
    SAFE_CAST(recommendation_product_id AS INTEGER) AS recommendation_product_id,  -- Convert to INTEGER
    SAFE_CAST(recommendation_clicked_position AS INTEGER) AS recommendation_clicked_position,
    ARRAY(
        SELECT AS STRUCT
            SAFE_CAST(option_id AS INTEGER) AS option_id,  -- Convert to INTEGER
            option_label,
            quality,
            quality_label,
            SAFE_CAST(value_id AS INTEGER) AS value_id,  -- Convert to INTEGER
            value_label,
            alloy,
            diamond,
            SAFE_CAST(shapediamond AS INTEGER) AS shapediamond  -- Convert to INTEGER
        FROM UNNEST(option)
    ) AS option
    ,
    ARRAY(
        SELECT AS STRUCT
            SAFE_CAST(product_id AS INTEGER) AS product_id,  -- Convert to INTEGER
            amount,
            SAFE_CAST(
                REGEXP_REPLACE(
                    REPLACE(price, ',', '.'),
                    r'(\d)\.(\d{3})(\.\d{2})', '$1$2$3'
                ) AS FLOAT64
            ) AS price,
            currency,
            ARRAY(
                SELECT AS STRUCT
                    SAFE_CAST(option_id AS INTEGER) AS option_id,  -- Convert to INTEGER
                    option_label,
                    quality,
                    quality_label,
                    SAFE_CAST(value_id AS INTEGER) AS value_id,  -- Convert to INTEGER
                    value_label,
                    alloy,
                    diamond,
                    SAFE_CAST(shapediamond AS INTEGER) AS shapediamond  -- Convert to INTEGER
                FROM UNNEST(option)
            ) AS option
        FROM UNNEST(cart_products)
    ) AS cart_products
FROM {{ source('summary', 'glamira_dataset') }}