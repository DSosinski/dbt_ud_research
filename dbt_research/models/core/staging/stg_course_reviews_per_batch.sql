with ud_reviews as (
    select distinct 
        url,   
        max(reviews) reviews,
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
tg_reviews as (
    select distinct
        url,   
        reviews,
        batch_no,
        batch_date,
        type 
    from 
        {{ref("src_tg_courses")}}  tab_tg_courses
    where
        not exists ( select 1 from ud_reviews where ud_reviews.url = tab_tg_courses.url and ud_reviews.batch_no = tab_tg_courses.batch_no )
)
select distinct 
    url,   
    reviews,
    batch_no,
    batch_date,
    type
from 
    ud_reviews
union
select distinct
    url,   
    reviews,
    batch_no,
    batch_date,
    type
from 
    tg_reviews