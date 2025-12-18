===================
Queries to Practice
===================
create table abcd(col1 int, col2 varchar(5));
create table abc(col1 varchar);
insert into abc values ('a'),('b'),('c'),('d'),('e'),('f'),('g'),('h');

alter table abc add column col2 number;
create sequence seq1;
update abc set col2=seq1.nextval;

select * from abc;
--------------------
select date_trunc('year', current_date);
select current_timestamp;
select date_trunc('year', current_timestamp);
select date_trunc('month', current_timestamp);
select date_trunc('day', current_timestamp);
--------------------

insert into abcd values(1,'abc'), (2,'xyz');
select * from abcd;

drop table abcd;

create table abcd(col1 int, col2 varchar(5), col3 date);
insert into abcd values(101,'abcde',current_date);
select * from abcd;

undrop table abcd;

alter table abcd rename to abcd_day2;
undrop table abcd;
alter table abcd rename to abcd_day1;
alter table abcd_day2 rename to abcd;

select * from abcd_day1;
select * from abcd;
----------------------

create or replahce table EMP_LANG(EMP_ID int, FULL_NAME varchar(50), LANG varchar(20));

insert into EMP_LANG values 
(101, 'Virat Kohli', 'English'), (101, 'Virat Kohli', 'Hindi'), (101, 'Virat Kohli', 'Marathi'),
(102, 'Mahesh Babu', 'English'), (102, 'Mahesh Babu', 'Telugu'), (102, 'Mahesh Babu', 'Tamil'), 
(102, 'Mahesh Babu', 'Hindi'), (103, 'Janardhan', 'Telugu'), (103, 'Janardhan', 'English');

select * from EMP_LANG;

select FULL_NAME, LISTAGG(LANG, ',') as LANG_SPEAK from EMP_LANG
group by FULL_NAME;

-------------------------

create or replace table tab(id int, val varchar(10));

insert into tab values 
(1, 'A'), (1, 'B'), (1, 'C'), (2, 'X'), (2, 'Y'), (3, 'A'), (3, 'B');

select * from tab;

select id, LISTAGG(val, '|') as "values" from tab  group by id;