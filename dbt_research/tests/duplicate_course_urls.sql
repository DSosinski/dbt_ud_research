select url
from {{ref("stg_courses")}}
group by 
    url
having count(1) > 1