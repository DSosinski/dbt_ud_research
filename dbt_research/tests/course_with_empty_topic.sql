select url
from {{ref('stg_course_topics')}}
where topic  = ''