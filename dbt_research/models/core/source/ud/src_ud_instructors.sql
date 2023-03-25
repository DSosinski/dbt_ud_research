select distinct 
    id,
    display_name,
    job_title,
    url,
    batch_no,
    batch_date,
    'UD' type
from
    {{source('udemy_data','ud_instructors')}}