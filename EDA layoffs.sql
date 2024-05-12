-- Data Cleaning
use world_layoffs;
select 
  * 
from 
  mylayoffs;
-- 1. remover duplicatas
-- 2. padronizar os dados
-- 3. tratar valores nulls e em branco
-- 4. remover informações desnecessária
create table layoffs_test like mylayoffs;
select 
  * 
from 
  layoffs_test;
insert into layoffs_test 
select 
  * 
from 
  mylayoffs;
Select 
  *, 
  row_number() over(
    partition by company, industry, total_laid_off, 
    percentage_laid_off, `date`, stage, 
    country, funds_raised_millions
  ) as row_num 
from 
  layoffs_test;
with duplicate_cte as (
  Select 
    *, 
    row_number() over(
      partition by company, industry, total_laid_off, 
      percentage_laid_off, `date`, stage, 
      country, funds_raised_millions
    ) as row_num 
  from 
    layoffs_test
) 
select 
  * 
from 
  duplicate_cte 
where 
  row_num > 1;
CREATE TABLE `mylayoffs2` (
  `company` text, `location` text, `industry` text, 
  `total_laid_off` bigint DEFAULT NULL, 
  `percentage_laid_off` text, `date` text, 
  `stage` text, `country` text, `funds_raised_millions` int DEFAULT NULL, 
  `row_num` int
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;
select 
  * 
from 
  mylayoffs2;
insert into mylayoffs2 
Select 
  *, 
  row_number() over(
    partition by company, industry, total_laid_off, 
    percentage_laid_off, `date`, stage, 
    country, funds_raised_millions
  ) as row_num 
from 
  layoffs_test;
select 
  * 
from 
  mylayoffs2 
where 
  row_num > 1;
delete from 
  mylayoffs2 
where 
  row_num > 1;
-- Padronizando os dados
select 
  company, 
  trim(company) 
from 
  mylayoffs2;
update 
  mylayoffs2 
set 
  company = trim(company);
select 
  distinct industry 
from 
  mylayoffs2 
order by 
  1;
select 
  * 
from 
  mylayoffs2 
where 
  industry like 'Crypto%';
update 
  mylayoffs2 
set 
  industry = 'Crypto' 
where 
  industry like 'Crypto%';
select 
  distinct location 
from 
  mylayoffs2 
order by 
  1;
update 
  mylayoffs2 
set 
  location = 'Florianópolis' 
where 
  industry = 'FlorianÃ³polis';
select 
  distinct country 
from 
  mylayoffs2 
order by 
  1;
update 
  mylayoffs2 
set 
  country = 'United States' 
where 
  country = 'United States.';
select 
  `date`, 
  str_to_date(`date`, '%m/%d/%Y') 
from 
  mylayoffs2;
update 
  mylayoffs2 
set 
  `date` = str_to_date(`date`, '%m/%d/%Y');
select 
  `date` 
from 
  mylayoffs2;
alter table 
  mylayoffs2 modify column `date` DATE;
select 
  * 
from 
  mylayoffs2 
where 
  industry is null 
  or industry = '';
select 
  * 
from 
  mylayoffs2 
where 
  company = 'Airbnb';
select 
  t1.company, 
  t1.industry, 
  t2.industry 
from 
  mylayoffs2 as t1 
  join mylayoffs2 as t2 on t1.company = t2.company 
where 
  (
    t1.industry is null 
    or t1.industry = ''
  ) 
  and t2.industry is not null;
update 
  mylayoffs2 
set 
  industry = null 
where 
  industry = '';
update 
  mylayoffs2 as t1 
  join mylayoffs2 as t2 on t1.company = t2.company 
set 
  t1.industry = t2.industry 
where 
  (t1.industry is null) 
  and t2.industry is not null;
delete from 
  mylayoffs2 
where 
  total_laid_off is null 
  and percentage_laid_off is null;
select 
  * 
from 
  mylayoffs2 
where 
  total_laid_off is null 
  and percentage_laid_off is null;
alter table 
  mylayoffs2 
drop 
  column row_num;
select 
  * 
from 
  mylayoffs2;
-- Análise exploratória
select 
  max(total_laid_off), 
  max(percentage_laid_off) 
from 
  mylayoffs2;
select 
  * 
from 
  mylayoffs2 
where 
  percentage_laid_off = 1 
order by 
  total_laid_off desc;
select 
  company, 
  sum(total_laid_off) 
from 
  mylayoffs2 
group by 
  company 
order by 
  2 desc;
select 
  min(`date`), 
  max(`date`) 
from 
  mylayoffs2;
select 
  industry, 
  sum(total_laid_off) 
from 
  mylayoffs2 
group by 
  industry 
order by 
  2 desc;
select 
  country, 
  sum(total_laid_off) 
from 
  mylayoffs2 
group by 
  country 
order by 
  2 desc;
select 
  year(`date`), 
  sum(total_laid_off) 
from 
  mylayoffs2 
group by 
  year(`date`) 
order by 
  1;
select 
  stage, 
  sum(total_laid_off) 
from 
  mylayoffs2 
group by 
  stage 
order by 
  2 desc;
select 
  substring(`date`, 1, 7) as `MONTH`, 
  SUM(total_laid_off) 
from 
  mylayoffs2 
where 
  substring(`date`, 1, 7) is not null 
group by 
  `MONTH` 
ORDER BY 
  1 asc;
with Rolling_Total as (
  select 
    substring(`date`, 1, 7) as `MONTH`, 
    SUM(total_laid_off) AS total_off 
  from 
    mylayoffs2 
  where 
    substring(`date`, 1, 7) is not null 
  group by 
    `MONTH` 
  ORDER BY 
    1 asc
) 
select 
  `MONTH`, 
  total_off, 
  sum(total_off) over (
    order by 
      `MONTH`
  ) AS rolling_total 
from 
  Rolling_Total;
select 
  company, 
  sum(total_laid_off) 
from 
  mylayoffs2 
group by 
  company 
order by 
  2 desc;
select 
  company, 
  year(`date`), 
  sum(total_laid_off) 
from 
  mylayoffs2 
group by 
  company, 
  `date` 
order by 
  3 desc;
with Company_Year(company, years, total_laid_off) as (
  select 
    company, 
    year(`date`), 
    sum(total_laid_off) 
  from 
    mylayoffs2 
  group by 
    company, 
    `date`
), 
Company_Year_Rank as(
  select 
    *, 
    dense_rank() over(
      partition by years 
      order by 
        total_laid_off desc
    ) as ranking 
  from 
    Company_Year 
  where 
    years is not null
) 
select 
  * 
from 
  Company_Year_Rank 
where 
  ranking <= 5;
