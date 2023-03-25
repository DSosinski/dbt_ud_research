
{{ config(
    materialized='table',
    format='parquet'
) }}

select 
*
from 
    {{ref("stg_course_reviews_per_batch")}}