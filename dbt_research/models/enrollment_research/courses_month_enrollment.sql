with main_course_list as (
    select 
        url,
        is_paid
    from {{ref("core_course_details")}}
), course_students as (
    select 
        url,
        students,
        batch_no
    from {{ref("core_course_students")}}
), course_recent_batch as (
    select url, max(batch_no) max_batch_no
    from (
        select 
        url,
        cast(batch_no as integer) batch_no
        from {{ref("core_course_students")}}
    )
    group by url
), course_first_batch as (
    select url, min(batch_no) min_batch_no
    from (
        select 
        url,
        cast(batch_no as integer) batch_no
        from {{ref("core_course_students")}}
    )
    group by url
)
select 
    main_course_list.url,
    c16.students current_students,
    c16.students - c15.students P15,
    c15.students - c14.students P14,
    c14.students - c13.students P13,
    c13.students - c12.students P12,
    c12.students - c11.students P11,
    c11.students - c10.students P10,
    c10.students - c9.students P9,
    c9.students - c8.students P8,
    c8.students - c7.students P7,
    c7.students - c6.students P6,
    c6.students - c5.students P5,
    c5.students - c4.students P4,
    c4.students - c3.students P3,
    c3.students - c2.students P2,
    c2.students - c1.students P1,
    course_recent_batch.max_batch_no,
    course_first_batch.min_batch_no,
    main_course_list.is_paid
from main_course_list
left join course_recent_batch on course_recent_batch.url = main_course_list.url
left join course_first_batch on course_first_batch.url = main_course_list.url
left join course_students c1 on c1.url = main_course_list.url and c1.batch_no = '1'
left join course_students c2 on c2.url = main_course_list.url and c2.batch_no = '2'
left join course_students c3 on c3.url = main_course_list.url and c3.batch_no = '3'
left join course_students c4 on c4.url = main_course_list.url and c4.batch_no = '4'
left join course_students c5 on c5.url = main_course_list.url and c5.batch_no = '5'
left join course_students c6 on c6.url = main_course_list.url and c6.batch_no = '6'
left join course_students c7 on c7.url = main_course_list.url and c7.batch_no = '7'
left join course_students c8 on c8.url = main_course_list.url and c8.batch_no = '8'
left join course_students c9 on c9.url = main_course_list.url and c9.batch_no = '9'
left join course_students c10 on c10.url = main_course_list.url and c10.batch_no = '10'
left join course_students c11 on c11.url = main_course_list.url and c11.batch_no = '11'
left join course_students c12 on c12.url = main_course_list.url and c12.batch_no = '12'
left join course_students c13 on c13.url = main_course_list.url and c13.batch_no = '13'
left join course_students c14 on c14.url = main_course_list.url and c14.batch_no = '14'
left join course_students c15 on c15.url = main_course_list.url and c15.batch_no = '15'
left join course_students c16 on c16.url = main_course_list.url and c16.batch_no = '16'