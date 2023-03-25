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
    cme.url,
    sum(cme.p1) p1,
    sum(cme.p2) p2,
    sum(cme.p3) p3,
    sum(cme.p4) p4,
    sum(cme.p5) p5,
    sum(cme.p6) p6,
    sum(cme.p7) p7,
    sum(cme.p8) p8,
    sum(cme.p9) p9,
    sum(cme.p10) p10,
    sum(cme.p11) p11,
    sum(cme.p12) p12,
    sum(cme.p13) p13,
    sum(cme.p14) p14
from inst
left join cme on cme.url = inst.course_url
where inst.instructor_name = 'Paweł Krakowiak'
group by inst.instructor_name
,cme.url
order by p14 desc