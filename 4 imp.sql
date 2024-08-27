-- data cleaning--
use proj1;
select * from layoffs;
SET SQL_SAFE_UPDATES = 0;

### copy the data to another table

create table df like layoffs;
insert into df 
select *
from layoffs;

select * from df;

#### removing dublicate

SELECT * , row_number() over(
partition by company,industry,total_laid_off,percentage_laid_off,'date') as row_num
from df;

with duplication as (
	SELECT * , row_number() over(
	partition by company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) as row_num
	from df
	)
	select * from duplication
	where row_num >1;


##  create anew table
CREATE TABLE `df2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


select * from df2;

insert into df2
SELECT * , row_number() over(
	partition by company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions) as row_num
	from df;


select * from df2 where row_num>1;
SET SQL_SAFE_UPDATES = 0;

##delete duplicate
delete from df2 where row_num>1;

-- ---------------------------------------
#remove trim
SELECT  company ,trim(company) from df2;

update df2
set company =trim(company)
;

select distinct company from df2;
-- ------------------------
select distinct industry from df2
order by 1;

select * from df2
where industry like 'Crypto%'
order by 1;

update df2
set industry='Crypto'
where industry like 'Crypto%';

-- -------------------------------------------
select distinct country from df2 order by 1;

update df2
set country ='United States'
where country like 'United States%'
;
	#################################################trailing ######################333
select country,trim(trailing '.' from country) from df;


#########################update date ###########################3

select `date` ,
str_to_date(`date`,'%m/%d/%Y') 
from df2;

update df2 
set `date` =str_to_date(`date`,'%m/%d/%Y')
;

select date from df2;

#################change column data type #############3

alter table df2 
modify column `date` date;


############## deal with null################33
SELECT * from df2
where industry is null 
or industry ='';


update df2
set industry = null
where industry='';

select d1.industry,d2.industry
from df2 d1
join df2 d2
	on d1.company=d2.company
where d1.industry is null and d2.industry is not null;

update df2 d1
join df2 d2
	on d1.company =d2.company
    
set d1.industry =d2.industry
where d1.industry is null and d2.industry is not null;

-- ---------------
select * from df2
where total_laid_off is null and percentage_laid_off is null
 ;

delete from df2 
where total_laid_off is null and percentage_laid_off is null
 ;
 
 select * from df2;
 
 ################Drop column#############333
 
 alter table df2
 drop row_num;
 
 ####################################exploratry data analysis###############################
 select company , avg(total_laid_off) 
 from df2
 group by company
 order by 2 desc;
 
 -- ------
select max(date) ,min(date)
from df2;
 
 
 ----
  select INDUSTRY , avg(total_laid_off) 
 from df2
 group by INDUSTRY	
 order by 2 desc;
 
 -- -------------
 select `date` , avg(total_laid_off) 
 from df2
 group by `date`	
 order by 1 desc;
 
 -- -------------------------
   select year(`date`) , avg(total_laid_off) 
 from df2
 group by year(`date`)	
 order by 1 desc;
 
 -- - ------------------------------
 select substring(`date`,6,2) as month,avg(total_laid_off)
 from df2
 group by month
 order by 1;
 -- ----------------------------------
  select substring(`date`,1,7) as month,avg(total_laid_off)
 from df2
 where substring(`date`,1,7) is not null
 group by month
 order by 1;