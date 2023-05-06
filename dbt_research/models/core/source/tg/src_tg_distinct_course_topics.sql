with topic_0 as (
    select  distinct topic
    from {{source('udemy_data','courses')}}
    where regexp_count(topic, ',')  = 0
),
topic_1 as (
    select split_part(topic, ',' ,1) topic
    from {{source('udemy_data','courses')}} 
    where regexp_count(topic, ',')  = 1 
    union
    select split_part(topic, ',' ,2) topic
    from {{source('udemy_data','courses')}} 
    where regexp_count(topic, ',')  = 1 
),
topic_2 as (
    select split_part(topic, ',' ,1) topic
    from {{source('udemy_data','courses')}} 
    where regexp_count(topic, ',')  = 2 
    union
    select split_part(topic, ',' ,2) topic
    from {{source('udemy_data','courses')}}
    where regexp_count(topic, ',')  = 2 
    union
    select split_part(topic, ',' ,3) topic
    from {{source('udemy_data','courses')}} 
    where regexp_count(topic, ',')  = 2 
),
topic_3 as (
    select split_part(topic, ',' ,1) topic
    from {{source('udemy_data','courses')}} 
    where regexp_count(topic, ',')  = 3 
    union
    select split_part(topic, ',' ,2) topic
    from {{source('udemy_data','courses')}}
    where regexp_count(topic, ',')  = 3 
    union
    select split_part(topic, ',' ,3) topic
    from {{source('udemy_data','courses')}} 
    where regexp_count(topic, ',')  = 3 
    union
    select split_part(topic, ',' ,4) topic
    from {{source('udemy_data','courses')}} 
    where regexp_count(topic, ',')  = 3
),
topic_4 as (
    select split_part(topic, ',' ,1) topic
    from {{source('udemy_data','courses')}} 
    where regexp_count(topic, ',')  = 4 
    union
    select split_part(topic, ',' ,2) topic
    from {{source('udemy_data','courses')}}
    where regexp_count(topic, ',')  = 4 
    union
    select split_part(topic, ',' ,3) topic
    from {{source('udemy_data','courses')}} 
    where regexp_count(topic, ',')  = 4 
    union
    select split_part(topic, ',' ,4) topic
    from {{source('udemy_data','courses')}} 
    where regexp_count(topic, ',')  = 4
    union
    select split_part(topic, ',' ,5) topic
    from {{source('udemy_data','courses')}} 
    where regexp_count(topic, ',')  = 4
),
topic_5 as (
    select split_part(topic, ',' ,1) topic
    from {{source('udemy_data','courses')}} 
    where regexp_count(topic, ',')  = 5 
    union
    select split_part(topic, ',' ,2) topic
    from {{source('udemy_data','courses')}}
    where regexp_count(topic, ',')  = 5 
    union
    select split_part(topic, ',' ,3) topic
    from {{source('udemy_data','courses')}} 
    where regexp_count(topic, ',')  = 5 
    union
    select split_part(topic, ',' ,4) topic
    from {{source('udemy_data','courses')}} 
    where regexp_count(topic, ',')  = 5
    union
    select split_part(topic, ',' ,5) topic
    from {{source('udemy_data','courses')}} 
    where regexp_count(topic, ',')  = 5
    union
    select split_part(topic, ',' ,6) topic
    from {{source('udemy_data','courses')}} 
    where regexp_count(topic, ',')  = 5
),
raw_topics as (
    select
        topic
    from topic_0
    union
    select 
        distinct topic
    from topic_1
    union
    select 
        distinct topic
    from topic_2
    union
    select 
        distinct topic
    from topic_3
    union
    select 
        distinct topic
    from topic_4
    union
    select 
        distinct topic
    from topic_5    
),
cleaned_topics as (
    select distinct
    trim(replace(topic, '(P)', '')) topic,
    0 udemy_id
from raw_topics
),
tg_topics as (
    select distinct topic, udemy_id from src_tg_topics
)
select 
    topic, udemy_id
from cleaned_topics
where not exists ( select 1 from tg_topics where tg_topics.topic = cleaned_topics.topic )
and topic is not null
union
select 
    topic, udemy_id
from tg_topics
order by udemy_id