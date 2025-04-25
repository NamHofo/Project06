
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

SELECT
    t.*
FROM {{ source('summary', 'glamira_dataset') }} t
CROSS JOIN UNNEST(t.option) AS opt
WHERE opt.option_id IS NOT NULL
LIMIT 1000



/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
