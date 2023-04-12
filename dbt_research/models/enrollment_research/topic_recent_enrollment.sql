with course_topics as (
    select 
        url,
        topic,
        is_paid
    from {{ref('core_course_topics')}}
    where is_paid = 'Yes'
), course_students as (
    select 
        url,
        students,
        batch_no
    from {{ref("core_course_students")}}
)
select 
    topic,
    count(distinct url) course_count,
    sum(students) students,
    sum(prev_students) prev_students,
    max(students) max_studends,
    round(cast(
            cast(max(students) as double)
            /
            cast(sum(students) as double) 
        as double ),2) * 100 max_perc
from (
    select 
    course_topics.url,
    course_topics.topic,
    course_topics.is_paid,
    c_recent.students - c_before.students students,
    c_before.students - c_prev.students prev_students
    from course_topics
    join course_students c_recent on c_recent.url = course_topics.url and c_recent.batch_no = 
    (
    select cast(max(batch_no) as varchar) batch_no  
        from (
            select 
            url,
            cast(batch_no as integer) batch_no
            from udemy_data_dev.core_course_students
        )
    )
    
    join course_students c_before on c_before.url = course_topics.url and c_before.batch_no = 
    (
    select cast(max(batch_no)-1 as varchar) batch_no  
        from (
            select 
            url,
            cast(batch_no as integer) batch_no
            from udemy_data_dev.core_course_students
        )
    )
    left join course_students c_prev on c_prev.url = course_topics.url and c_prev.batch_no = 
    (
    select cast(max(batch_no)-2 as varchar) batch_no  
        from (
            select 
            url,
            cast(batch_no as integer) batch_no
            from udemy_data_dev.core_course_students
        )
    )
)
group by topic