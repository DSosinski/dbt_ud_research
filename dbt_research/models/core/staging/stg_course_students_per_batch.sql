with ud_students as (
    select distinct 
        url,   
        max(students) students,
        batch_no,
        batch_date,
        type 
    from 
        {{ref("src_ud_courses")}}
    group by 
        url,
        batch_no,
        batch_date,
        type
),
tg_students as (
    select distinct
        url,   
        students,
        batch_no,
        batch_date,
        type 
    from 
        {{ref("src_tg_courses")}} tab_tg_courses  
    where
        not exists ( select 1 from ud_students where ud_students.url = tab_tg_courses.url and ud_students.batch_no = tab_tg_courses.batch_no )
)
select distinct 
    url,   
    students,
    batch_no,
    batch_date,
    type
from 
    ud_students 
union
select distinct
    url,   
    students,
    batch_no,
    batch_date,
    type
from 
    tg_students 