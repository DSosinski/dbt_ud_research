select 
    ' ' id,
    course_url url,
    title,
    author,
    author_id,
    topic,
    topic_id,
    students,
    reviews,
    published,
    created,
    last_updated,
    duration,
    is_free,    
    batch_no,
    batch_date,
    'TG' type
from
    {{source('udemy_data','courses')}}