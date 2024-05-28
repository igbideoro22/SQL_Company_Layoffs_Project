SELECT *
FROM world_layoffs.layoffs;

-- STEP 1: CLEAN THE DATA

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT INTO layoffs_staging
SELECT *
FROM layoffs;

-- USE A CTE TO DETECT DUPLICATE
WITH `New_Row` AS (SELECT *, ROW_NUMBER() OVER(PARTITION BY `company`, `location`,`industry`,`total_laid_off`,
`percentage_laid_off`,`date`,`stage`,`country`,`funds_raised_millions`) AS `Row Number` FROM layoffs_staging)

SELECT *
FROM `New_Row`
WHERE `Row Number` > 1
;

SELECT *
FROM layoffs_staging
WHERE `company` LIKE 'Cazoo';

-- OR USE A TEMPORARY TABLE TO DETECT DUPLICATES
CREATE TEMPORARY TABLE layoffs_staging_C
SELECT *, ROW_NUMBER() OVER(PARTITION BY `company`, `location`,`industry`,`total_laid_off`,
`percentage_laid_off`,`date`,`stage`,`country`,`funds_raised_millions`) AS `Row Number`
FROM layoffs_staging;

-- FIND AND DELETE DUPLICATES, USE SELECT FIRST
DELETE
FROM layoffs_staging_C
WHERE `Row Number` > 1
;

-- TRANSFER THE CLEANED FILES INTO A NEW TABLE SO YOU CAN STILL ACCESS RAW TABLE
CREATE TABLE layoffs_staging_Cleaned
SELECT * 
FROM layoffs_staging_C;

SELECT *
FROM layoffs_staging_Cleaned;

-- STEP 2: STANDARDISING DATA
-- REMOVING AND TRIMMING SPACES 

SELECT company, TRIM(company)
FROM layoffs_staging_Cleaned;

UPDATE layoffs_staging_Cleaned
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging_Cleaned
ORDER BY industry;

SELECT *
FROM layoffs_staging_Cleaned;

CALL `find`('support%');

UPDATE layoffs_staging_Cleaned
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

CALL `trim`('industry');

-- CHANGE DATE COLUMN TO DATE FORMAT
UPDATE layoffs_staging_Cleaned
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- TRAILING '' IS USED TO IDENTIFY A CHARACTER
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging_Cleaned
ORDER BY 1;

UPDATE layoffs_staging_Cleaned
SET country = TRIM(TRAILING '.' FROM country);

-- STEP 3: NULL AND BLANK VALUES. USE 'IS' KEYWORD TO FIND NULL VALUES
-- DELETE ROWS WITH IRRELEVANT DATA
DELETE FROM layoffs_staging_Cleaned
WHERE funds_raised_millions IS NULL
AND total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * FROM layoffs_staging_Cleaned
WHERE industry = '' OR industry IS NULL;

UPDATE layoffs_staging_Cleaned
SET industry = NULL
WHERE industry = '';

SELECT * FROM layoffs_staging_Cleaned
WHERE company = 'Airbnb' AND location = 'SF Bay Area' AND country = 'United States';


-- FIND THE BLANK OR NULL industry sections
SELECT * 
FROM layoffs_staging_Cleaned  
JOIN layoffs_staging_Cleaned AS T2
	ON T1.company = T2.company AND T1.location = T2.location AND T1.country = T2.country
WHERE T1.industry IS NULL AND T2.industry IS NOT NULL;

-- UPDATE THE industry section
UPDATE layoffs_staging_Cleaned AS T1
JOIN layoffs_staging_Cleaned AS T2
	ON T1.company = T2.company AND T1.location = T2.location AND T1.country = T2.country
SET T1.industry = T2.industry
WHERE T1.industry IS NULL AND T2.industry IS NOT NULL;

-- EXPLORING THE DATA USING CTE
-- FINDING THE MIN, MAX AND AVERAGE OF THE DATA
WITH max_layoff_cte AS (SELECT MAX(total_laid_off) AS max_layoff, MIN(total_laid_off) AS min_layoff, AVG(total_laid_off) AS avg_layoff, 
MAX(percentage_laid_off) AS max_perc_layoff, MIN(percentage_laid_off) AS min_perc_layoff
FROM layoffs_staging_Cleaned) 

SELECT *
FROM max_layoff_cte;

SELECT company, total_laid_off, percentage_laid_off, max_layoff
FROM layoffs_staging_Cleaned, max_layoff_cte
WHERE total_laid_off = max_layoff
;

SELECT *
FROM layoffs_staging_Cleaned;

-- GROUP SUM BY YEAR

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging_Cleaned
GROUP BY YEAR(`date`);

-- GROUP SUM BY COUNTRY 

SELECT country, SUM(total_laid_off)
FROM layoffs_staging_Cleaned
GROUP BY country
ORDER BY 2 DESC;

-- TOP 5 COMPANIES LAY OFF WORLDWIDE PER YEAR

WITH Company_Group AS
(SELECT company, industry, YEAR(`date`) AS `Year`, SUM(total_laid_off) AS `total number laid off in the Year`
FROM layoffs_staging_Cleaned
GROUP BY company, industry, `Year`),

Company_total AS 
(SELECT *, DENSE_RANK() OVER(PARTITION BY `Year` ORDER BY `total number laid off in the Year` DESC) AS `Ranking`
FROM Company_Group
WHERE `total number laid off in the Year` IS NOT NULL AND `Year` IS NOT NULL)

SELECT *
FROM Company_total
WHERE `Ranking` <= 5
;

-- TOP 5 INDUSTRIES WORLDWIDE LAY OFF PER YEAR

WITH industry_layoffs AS
(SELECT industry, YEAR(`date`) AS `year`, SUM(total_laid_off) AS `sum_total_laid_off`
FROM layoffs_staging_Cleaned
GROUP BY industry, YEAR(`date`)),

industry_layoffs_ranking AS
(SELECT *, RANK() OVER(PARTITION BY `year` ORDER BY `sum_total_laid_off` DESC) AS `Ranking`
FROM industry_layoffs
WHERE `sum_total_laid_off` IS NOT NULL AND `industry` NOT LIKE 'Other' AND  `year` IS NOT NULL)

SELECT *
FROM industry_layoffs_ranking
WHERE `Ranking` <= 5;



-- RANGE OF THE DATA SET
SELECT SUBSTR(`date`, 1,7) AS `sub`
FROM layoffs_staging_Cleaned
WHERE 1 IS NOT NULL
GROUP BY 1
ORDER BY 1;


-- TOP 5 FUND RAISERS WORLDWIDE PER YEAR
WITH fund_raisers AS
(SELECT company, industry, YEAR(`date`) AS `Year`, SUM(funds_raised_millions) AS `funds`
FROM layoffs_staging_Cleaned
GROUP BY company, industry, YEAR(`date`)),
fund_raisers2 AS
(SELECT *, DENSE_RANK() OVER(PARTITION BY `Year` ORDER BY `funds` DESC) AS `Ranking`
FROM fund_raisers
WHERE `Year` IS NOT NULL AND `funds`IS NOT NULL)
SELECT *
FROM fund_raisers2
WHERE `Ranking` <= 5;


-- TOP FUND RAISERS IN A COUNTRY PER YEAR
WITH Country_funds AS
(SELECT company, industry, country, YEAR(`date`) AS `Year`, SUM(funds_raised_millions) AS `Funds`
FROM layoffs_staging_Cleaned
GROUP BY company, industry, country, YEAR(`date`)),
Country_funds_Ranked AS
(SELECT *, DENSE_RANK() OVER(PARTITION BY country, `Year` ORDER BY Funds DESC) AS `Ranking`
FROM Country_funds
WHERE `Funds` IS NOT NULL AND `Year` IS NOT NULL)
SELECT *
FROM Country_funds_Ranked
WHERE `Ranking` = 1;


