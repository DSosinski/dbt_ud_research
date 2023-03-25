
{{ config(
    materialized='table',
    format='parquet'
) }}

select 
*
from 
    {{ref("stg_course_students_per_batch")}}