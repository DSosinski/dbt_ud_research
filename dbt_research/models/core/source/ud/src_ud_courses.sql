select 
    id,
     ('https://www.udemy.com' || url) url,
    title,
    topic topic_id,
    num_subscribers students,
    num_reviews reviews,    
    published_time published,
    created,
    last_update_date last_updated,
    content_info_short duration,
    is_paid,
    batch_no,
    batch_date,
    'UD' type
from
    {{source('udemy_data','ud_courses')}}