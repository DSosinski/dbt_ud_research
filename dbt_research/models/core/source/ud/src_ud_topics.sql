select 
    id,
    title
from
    {{source('udemy_data','ud_topics')}}