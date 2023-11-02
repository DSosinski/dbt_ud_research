with stg_courses as (
    select distinct 
        url,
        is_paid
    from {{ref('stg_courses')}}
        
), ud_courses as (
    select distinct
        url,
        topic_id
    from {{ ref('src_ud_courses')}}    
), udemy_topics as (
    select distinct
        id udemy_id,
        title topic
    from {{ref('src_ud_topics')}} 
    where id <> -1
 ), topics as (
    select distinct
        udemy_id,
        topic_id,
        topic
    from {{ref('src_tg_topics')}} 
) ,tg_courses as (
    select  distinct
        src_tg_courses.url,
        substr(src_tg_courses.topic, 1, (strpos(src_tg_courses.topic, '(P)') - 1)) alt_topic,
        topics.topic,
        case src_tg_courses.topic 
            when '' then 'Unknown'
            else src_tg_courses.topic 
        end full_topic
    from {{ref('src_tg_courses')}} src_tg_courses
    left join topics on topics.topic_id = src_tg_courses.topic_id
    where not exists ( select 1 from ud_courses where ud_courses.url = src_tg_courses.url)
)
select 
    url,
    is_paid,
    case topic
    when '' then trim(full_topic)
    else trim(topic)
    end topic
from(
    select distinct
        stg_courses.url,
        stg_courses.is_paid,
        COALESCE(
            COALESCE (
                    COALESCE( 
                        COALESCE(udemy_topics.topic,topics.topic ),
                        tg_courses.topic
                            ),
                    tg_courses.alt_topic
                ),'Unknown') topic,
        tg_courses.full_topic    
    from stg_courses 
    left join ud_courses on stg_courses.url = ud_courses.url
    left join udemy_topics on ud_courses.topic_id = udemy_topics.udemy_id
    left join topics on ud_courses.topic_id = topics.udemy_id
    left join tg_courses on tg_courses.url = stg_courses.url
    )