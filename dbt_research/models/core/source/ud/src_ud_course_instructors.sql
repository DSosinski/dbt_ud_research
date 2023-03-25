select  
    id,
    course_id,
    batch_no,
    batch_date,
    'UD' type
from
    {{source('udemy_data','ud_instructors')}}