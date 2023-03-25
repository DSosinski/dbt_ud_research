select 
    url, batch_no, count(1)
from 
    {{ref('stg_course_students_per_batch')}}
group by 
    url, batch_no
having count(1) > 1