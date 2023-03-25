with stg_courses as (
    select distinct
        url,
        type,
        id 
    from {{ref('stg_courses')}}
),
src_ud_instructors as (
    select distinct
        url,
        display_name,
        id
    from  {{ref('src_ud_instructors')}}
),
src_ud_course_instructors as (
    select distinct
        course_id,
        id
     from  {{ref('src_ud_course_instructors')}}
),
stg_ud_instructors as (
    select distinct
        stg_courses.url course_url,
        stg_courses.type,
        src_ud_instructors.url instructor_url,
        src_ud_instructors.display_name instructor_name
    from stg_courses
    join src_ud_course_instructors on 
        src_ud_course_instructors.course_id = stg_courses.id 
    join src_ud_instructors on
         src_ud_instructors.id = src_ud_course_instructors.id 
    where stg_courses.type = 'UD'

),
src_tg_authors as (
    select distinct  
        author_id,
        author,
        author_url
     from  {{ref('src_tg_authors')}}
),
stg_tg_course_instructors as (
    select distinct 
    src_tg_courses.url course_url,
    stg_courses.type,
    src_tg_courses.author instructor_name,
    src_tg_authors.author_url instructor_url
    from stg_courses
    join {{ref('src_tg_courses')}}  src_tg_courses on
        stg_courses.url = src_tg_courses.url
    left join src_tg_authors on src_tg_authors.author = src_tg_courses.author
    where stg_courses.type = 'TG'
)
select 
*
from 
stg_ud_instructors
union
select 
*
from 
stg_tg_course_instructors