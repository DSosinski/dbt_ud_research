with ud_courses as (
    select distinct  
        ('https://www.udemy.com' || url) url,
        'UD' type
    from
        {{source('udemy_data','ud_courses')}}
),
tg_courses as(
    select distinct
        course_url ,
        'TG' type
    from
        {{source('udemy_data','courses')}}
    where 
        course_url not in (select url from ud_courses)

)
select 
    url,
    type 
from ud_courses
union
select  
    course_url url,
    type
from tg_courses
