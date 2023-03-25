select 
    topic_id,
    udemy_id,   
    topic,
    batch_no,
    batch_date,
    'TG' type
from
    {{source('udemy_data','topics')}}