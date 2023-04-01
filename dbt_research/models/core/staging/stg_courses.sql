    with stg_courses as (

        select url,type from {{ref('src_course_urls')}}
    ),
    ud_courses as (
        select distinct 
        url,
        id ,
        title,
        cast(duration as varchar) duration,
        cast(published as varchar) published,
        case 
            when is_paid = true then 'Yes'
            else 'No' 
        end is_paid,
        type
        from 
        {{ref('src_ud_courses')}} 
    ),
    tg_courses as (
        select distinct 
        url,
        id ,
        title,
        cast(duration as varchar) duration,
        cast(published as varchar) published,
        case 
            when is_free = 'Yes' then 'No'
            else 'Yes'
        end is_paid,
        type
        from 
        {{ref('src_tg_courses')}} 
    )
select distinct 
    stg_courses.url,
    ud_courses.type,
    max(ud_courses.id) id,
    max(ud_courses.title) title,
    max(ud_courses.duration) duration,
    max(ud_courses.published) published,
    max(ud_courses.is_paid) is_paid
from stg_courses
join ud_courses on stg_courses.url = ud_courses.url and stg_courses.type = ud_courses.type
group by stg_courses.url,ud_courses.type
union 
select distinct 
    stg_courses.url,
    tg_courses.type,
    0 id,
    max(tg_courses.title) title,
    max(tg_courses.duration) duration,
    max(tg_courses.published) published,
    max(tg_courses.is_paid) is_paid
from stg_courses
join tg_courses on stg_courses.url = tg_courses.url and stg_courses.type = tg_courses.type
group by stg_courses.url,  tg_courses.type, tg_courses.id