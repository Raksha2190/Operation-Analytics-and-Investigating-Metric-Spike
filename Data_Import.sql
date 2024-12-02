create database jd_analysis;
use jd_analysis;

select * from job_data;

create database IMS_Project2;
use IMS_Project2;

#Easy method to import csv files with lakhs of data
#**Load data inline will not work if any blank values - Go to each csv file and check for blank values 
#by clicking "Find and select" under home tab
#and then click on "special" and then "blank" and say ok
#all blanks will be highlighted and now click "delete sheet rows" under home tab.
 

create table users(
user_id int,
created_at varchar(100),
company_id int,
language varchar(50),
activated_at varchar(100),
state varchar(50));

create table events(
user_id int,
occured_at varchar(100),
event_type varchar(50),
event_name varchar(100),
location varchar(50),
device varchar(50),
user_type int
);

create table email_events(
user_id int,
occured_at varchar(100),
action varchar(100),
user_type int
);

show variables like 'secure_file_priv';

load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/users.csv'
into table users
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

select * from users;

load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv'
into table events
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

select * from events;

load data infile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/email_events.csv'
into table email_events
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;

select * from email_events;

#optional - not required here
alter table users
add column temp_created_at datetime;

update users set temp_created_at = str_to_date(created_at,'%d-%m-%Y %H:%i');

alter table users drop column created_at;
alter table users change column temp_created_at created_at datetime;

SELECT * FROM EMAIL_EVENTS;
SELECT * FROM EVENTS;
SELECT * FROM USERS;