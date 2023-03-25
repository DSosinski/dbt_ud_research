select     
    author_id,
    udemy_id,   
    author,
    author_url,
    batch_no,
    batch_date,
    'TG' type
from
    {{source('udemy_data','authors')}}