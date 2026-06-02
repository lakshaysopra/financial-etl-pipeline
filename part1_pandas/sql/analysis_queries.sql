---- 🔹 SALES & SCALE ANALYSIS ----

-- 1 Top 10 companies by total sales for latest year

SELECT DISTINCT company_name, sales,financial_year
FROM company_financials
WHERE sales IS NOT NULL
AND financial_year = 2023
ORDER BY sales DESC
LIMIT 10;

-- 2 top 10 Fastest growing companies by CAGR 

SELECT DISTINCT company_name, cagr,sales
FROM company_financials
WHERE cagr IS NOT NULL
AND financial_year = 2023
ORDER BY cagr DESC
LIMIT 10;

-- NOTE:
-- Initial CAGR ranking produced extremely high and unrealistic growth values
-- for certain companies. This occurred because CAGR is highly sensitive to
-- very small base-year sales values. Companies starting with near-zero sales
-- can generate artificially inflated CAGR percentages despite having relatively
-- small absolute business scale.
--
-- To improve analytical reliability and business relevance, a minimum sales
-- threshold filter (sales > 1000) was applied. This removes micro-scale
-- outliers and ensures the analysis focuses on financially meaningful,
-- operationally significant companies.
--
-- This approach reflects real-world financial analytics practices where
-- outlier filtering and base-value normalization are important for generating
-- interpretable growth comparisons.

SELECT DISTINCT company_name, sales, cagr
FROM company_financials
WHERE cagr IS NOT NULL
AND sales > 1000
AND financial_year = 2023
ORDER BY cagr DESC
LIMIT 10;



-- 3 Year with highest total industry sales

SELECT financial_year,
       SUM(sales) AS total_sales
FROM company_financials
WHERE sales IS NOT NULL
GROUP BY financial_year
ORDER BY total_sales DESC;


-- 4 Companies with continuously increasing sales

SELECT company_name
FROM company_financials
GROUP BY company_name
HAVING MIN(revenue_growth) > 0;


-- 5 Companies with continuously declining sales 

SELECT company_name
FROM company_financials
GROUP BY company_name
HAVING max(revenue_growth) < 0;


-- 6 Top companies by net profit margin

select distinct company_name,profit_growth
from company_financials
where financial_year = 2023 and profit_growth is not null
order by profit_growth desc 
limit 10;


-- 7 Companies with negative profit margins

SELECT company_name, profit_margin
FROM company_financials
WHERE profit_margin < 0
AND financial_year = 2023
ORDER BY profit_margin;


-- 8️ Most operationally efficient companies (highest OPM)

SELECT company_name, opm
FROM company_financials
WHERE opm IS NOT NULL
AND financial_year = 2023
ORDER BY opm DESC
LIMIT 10;


-- 9 Companies with lowest expense ratio

SELECT company_name, expense_ratio
FROM company_financials
WHERE expense_ratio IS NOT NULL
AND financial_year = 2023
ORDER BY expense_ratio ASC
LIMIT 10;


-- 10 Compare sales vs profit leaders

SELECT company_name,
       sales,
       net_profit
FROM company_financials
WHERE financial_year = 2023
ORDER BY sales DESC, net_profit DESC
LIMIT 10;









 




