use jd_analysis;

#Query 1 - Jobs Reviewed Over Time

select * from job_data;

# Non distinct job id
select count(job_id)/(30*24) as jobs_reviewed_per_hour_non_distinct from job_data
where ds between '11/01/2020' and '11/30/2020';

# With distinct job id
select count(distinct job_id)/(30*24) as jobs_reviewed_per_hour_distinct from job_data
where ds between '11/01/2020' and '11/30/2020'; 

#Query 2 - Throughput Analysis

select * from job_data;

select 
ds as date_of_review, 
count(job_id) as jobs_reviewed, 
avg(count(job_id)) over(order by ds rows between 6 preceding and current row) as throughput_rolling_7_avg
from job_data
group by ds
order by ds;

#Query 3 - Language Share Analysis

select * from job_data;

SELECT 
    language,
    COUNT(language) AS each_language_count,
    (COUNT(language) / (SELECT 
            COUNT(*)
        FROM
            job_data)) * 100 AS Percentage_share_each_language
FROM
    job_data
    where ds between (select max(ds) from job_data) - 29 and (select max(ds) from job_data)
GROUP BY language
ORDER BY language DESC;

#OR

select language,count(*) * 100.0/sum(count(*)) over() as language_percentage
from job_data
where ds between (select max(ds) from job_data) - 29 and (select max(ds) from job_data)
group by language;
 

#Query 4 - Duplicate Rows Detection

select * from job_data;

select * from 
(select *,row_number() over(partition by job_id) as row_num from job_data) as a    #SHOWS ONLY DUPLICATE ROWS
where row_num>1;

#OR

select *,row_number() over() as row_num from job_data   #SHOWS ALL 3 ROWS(WHICH INCLUDES ORIGINAL AND 2 DUPLICATES)
where job_id in 
(select job_id from job_data 
group by job_id 
having count(job_id)>1);

#Case Study 2: Investigating Metric Spike

use ims_project2;

#Task 1 - Weekly User Engagement
select * from users;
select * from events;

select week(str_to_date(occured_at,'%Y-%m-%d')) as week_no, 
count(distinct user_id) as active_users
from events
group by week_no
order by week_no;

#Task 2 - User Growth Analysis

select * from users;

select extract(year from (str_to_date(activated_at,'%d-%m-%Y %H:%i'))) as registered_year,
extract(month from (str_to_date(activated_at,'%d-%m-%Y %H:%i'))) as registered_month,
count(distinct user_id) as active_users,
sum(count(distinct user_id)) over(order by 
(select extract(year from (str_to_date(activated_at,'%d-%m-%Y %H:%i'))) as registered_year),
(select extract(month from (str_to_date(activated_at,'%d-%m-%Y %H:%i'))) as registered_month) 
rows between unbounded preceding and current row) as cumulative_active_users
from users
group by registered_year,registered_month
order by registered_year,registered_month;

#Task 3 - Weekly Retention Analysis

SELECT DISTINCT
    user_id, COUNT(user_id) AS user_count,
    SUM(CASE
        WHEN retention_week = 1 THEN 1
        ELSE 0
    END) AS per_week_retention
FROM
    (SELECT 
        s.user_id, s.signup_week, e.engagement_week, e.engagement_week - s.signup_week AS retention_week
    FROM
        ((SELECT DISTINCT
        user_id, EXTRACT(WEEK FROM (STR_TO_DATE(occured_at, '%d-%m-%Y %H:%i'))) AS signup_week
    FROM events
    WHERE
        event_type = 'signup_flow' AND event_name = 'complete_signup') AS s
    LEFT JOIN (SELECT DISTINCT
        user_id, EXTRACT(WEEK FROM (STR_TO_DATE(occured_at, '%d-%m-%Y %H:%i'))) AS engagement_week
    FROM events
    WHERE event_type = 'engagement') AS e ON s.user_id = e.user_id)) AS a
GROUP BY user_id
ORDER BY user_id;

#Task 4 - Weekly Engagement Per Device
select * from events;

select EXTRACT(year FROM (STR_TO_DATE(occured_at, '%d-%m-%Y %H:%i'))) AS year, 
EXTRACT(WEEK FROM (STR_TO_DATE(occured_at, '%d-%m-%Y %H:%i'))) AS week_no, 
device, 
count(distinct user_id) as engaged_users
from events
where event_type="engagement"
group by year,week_no,device
order by year,week_no,device;

#Task 5 - Email Engagement Analysis

select * from email_events;

select count(distinct user_id) as user_count,
action, 
count(distinct user_id)*100.0/sum(count(distinct user_id)) over() as action_percentage
from email_events
group by action;

select sum(case when email_action='email_opened' then 1 else 0 end)/sum(case when email_action='email_sent' then 1 else 0 end)*100 as email_opening_rate,
sum(case when email_action='email_clicked' then 1 else 0 end)/sum(case when email_action='email_sent' then 1 else 0 end)*100 as email_clicking_rate
from 
(select *, case when action in ('email_open')
then 'email_opened'
when action in ('sent_weekly_digest','sent_reengagement_email')
then 'email_sent'
when action in ('email_clickthrough')
then 'email_clicked'
end as email_action
from email_events) as email_engagement;