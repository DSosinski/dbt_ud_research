{{ config(
    materialized='table',
    format='parquet'
) }}

with cme as (
    select 
    *
    from {{ ref("courses_month_enrollment")}}
    where is_paid = 'Yes'
), inst as
(
    select 
    *
    from {{ ref("core_course_instructors")}}
)
select 
    inst.instructor_name,
    inst.instructor_url,
    count(distinct cme.url) course_count,
    sum(cme.current_students) current_students,
    sum(cme.p22) P22,
    sum(cme.p21) P21,
    sum(cme.p20) P20,
    sum(cme.p19) p19,
    sum(cme.p18) p18,
    sum(cme.p17) p17,
    sum(cme.p16) p16,
    sum(cme.p15) p15,
    sum(cme.p14) p14,
    sum(cme.p13) p13,
    sum(cme.p12) p12,
    sum(cme.p11) p11,
    sum(cme.p10) p10,
    sum(cme.p9) p9,
    sum(cme.p8) p8,
    sum(cme.p7) p7,
    sum(cme.p6) p6,
    sum(cme.p5) p5,
    sum(cme.p4) p4,
    sum(cme.p3) p3,
    sum(cme.p2) p2,
    sum(cme.p1) p1
from inst
left join cme on cme.url = inst.course_url
group by inst.instructor_name, inst.instructor_url
