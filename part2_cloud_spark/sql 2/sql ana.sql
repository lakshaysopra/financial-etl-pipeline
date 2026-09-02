-- 1. Top 10 companies by sales — latest year

SELECT company_name, sales, year
FROM company_financials
WHERE sales IS NOT NULL
  AND year = 2023
ORDER BY sales DESC
LIMIT 10;


-- 2. Total industry sales by year

SELECT
    year,
    SUM(sales) AS total_sales
FROM company_financials
WHERE sales IS NOT NULL
GROUP BY year
ORDER BY total_sales DESC;


-- 3. Companies with continuously increasing sales 

 
SELECT company_name
FROM company_financials
GROUP BY company_name
HAVING MIN(revenue_growth) > 0
ORDER BY company_name;


-- 4. Continuously decreasing sales:

SELECT company_name
FROM company_financials
GROUP BY company_name
HAVING MAX(revenue_growth) < 0
ORDER BY company_name;


-- 5. Rank Companies by Sales (top 10 of all years)

SELECT
    company_name,
    year,
    sales,
    sales_rank
FROM (
    SELECT
        company_name,
        year,
        sales,
		RANK() OVER (
            PARTITION BY year
            ORDER BY sales DESC
        ) AS sales_rank
	FROM company_financials
    WHERE sales IS NOT NULL
) ranked
WHERE sales_rank <= 10
ORDER BY year desc, sales_rank;


-- 2.1 Top 10 Companies by Net Profit

SELECT
    company_name,
    sales,
    net_profit,
    profit_margin
FROM company_financials
WHERE year = 2023
  AND net_profit IS NOT NULL
ORDER BY net_profit DESC
LIMIT 10;


-- 2.2 Highest Profit Margin

SELECT
    company_name,
    sales,
    net_profit,
    profit_margin
FROM company_financials
WHERE year = 2023
  AND profit_margin IS NOT NULL
  AND sales > 1000
ORDER BY profit_margin DESC
LIMIT 10;


-- 2.3 Highest Operating Margin

SELECT
    company_name,
    sales,
    operating_profit,
    opm
FROM company_financials
WHERE year = 2023
  AND opm IS NOT NULL
ORDER BY opm DESC
LIMIT 10;


-- 2.4 Loss-Making Companies

SELECT
    company_name,
    sales,
    net_profit,
    profit_margin
FROM company_financials
WHERE year = 2023
  AND net_profit < 0
ORDER BY net_profit ASC;


-- 2.5 top 10 net profit coopanies every yaer
SELECT
    company_name,
    year,
    net_profit,
    profit_rank
FROM (
    SELECT
        company_name,
        year,
        net_profit,

        RANK() OVER (
            PARTITION BY year
            ORDER BY net_profit DESC
        ) AS profit_rank

    FROM company_financials
    WHERE net_profit IS NOT NULL
) ranked
WHERE profit_rank <= 10
ORDER BY year, profit_rank;

-- 3.1 Lowest Expense Ratio

SELECT
    company_name,
    sales,
    expenses,
    expense_ratio
FROM company_financials
WHERE year = 2023
  AND expense_ratio IS NOT NULL
  AND sales > 1000
ORDER BY expense_ratio ASC
LIMIT 10;


-- 3.2 Highest Expense Ratio

SELECT
    company_name,
    sales,
    expenses,
    expense_ratio
FROM company_financials
WHERE year = 2023
  AND expense_ratio IS NOT NULL
  AND sales > 1000
ORDER BY expense_ratio DESC
LIMIT 10;



-- 3.3 Highest Operating Leverage

SELECT
    company_name,
    sales,
    operating_profit,
    operating_leverage
FROM company_financials
WHERE year = 2023
  AND operating_leverage IS NOT NULL
  AND operating_profit > 100
ORDER BY operating_leverage DESC
LIMIT 10;


-- 3.4 Highest Depreciation Burden

SELECT
    company_name,
    sales,
    depreciation,
    depreciation_ratio
FROM company_financials
WHERE year = 2023
  AND depreciation_ratio IS NOT NULL
  AND sales > 0
  AND depreciation >= 0
ORDER BY depreciation_ratio DESC
LIMIT 10;


-- 4.1 Weakest Interest Coverage

SELECT
    company_name,
    operating_profit,
    interest,
    interest_coverage
FROM company_financials
WHERE year = 2023
  AND interest_coverage IS NOT NULL
ORDER BY interest_coverage ASC
LIMIT 10;


-- 4.2 Strongest Interest Coverage

SELECT
    company_name,
    operating_profit,
    interest,
    interest_coverage
FROM company_financials
WHERE year = 2023
  AND interest_coverage IS NOT NULL
ORDER BY interest_coverage DESC
LIMIT 10;


-- 4.3 Highest Interest Burden

SELECT
    company_name,
    interest,
    interest_burden_ratio,
    net_profit
FROM company_financials
WHERE year = 2023
  AND interest_burden_ratio IS NOT NULL
ORDER BY interest_burden_ratio DESC
LIMIT 10;


-- 4.4 Highest Effective Tax Rate

SELECT
    company_name,
    profit_before_tax,
    net_profit,
    effective_tax_rate
FROM company_financials
WHERE year = 2023
  AND effective_tax_rate IS NOT NULL
ORDER BY effective_tax_rate DESC
LIMIT 10;



-- 5.1 Highest Revenue Growth

SELECT
    company_name,
    sales,
    revenue_growth
FROM company_financials
WHERE year = 2023
  AND revenue_growth IS NOT NULL
  AND sales > 1000
ORDER BY revenue_growth DESC
LIMIT 10;


-- 5.2 Highest CAGR

SELECT
    company_name,
    sales,
    cagr
FROM company_financials
WHERE year = 2023
  AND cagr IS NOT NULL
  AND sales > 1000
ORDER BY cagr DESC
LIMIT 10;


-- 5.3 Highest Profit Growth

SELECT
    company_name,
    net_profit,
    profit_growth
FROM company_financials
WHERE year = 2023
  AND profit_growth IS NOT NULL
ORDER BY profit_growth DESC
LIMIT 10;



-- 5.4 Highest EPS Growth

SELECT
    company_name,
    eps_in_rs,
    eps_growth
FROM company_financials
WHERE year = 2023
  AND eps_growth IS NOT NULL
ORDER BY eps_growth DESC
LIMIT 10;


-- 5.5 Strong Revenue + Profit Growth

SELECT
    company_name,
    sales,
    revenue_growth,
    net_profit,
    profit_growth
FROM company_financials
WHERE year = 2023
  AND revenue_growth > 10
  AND profit_growth > 10
ORDER BY revenue_growth desc,profit_growth DESC;



-- 5.6 Profit Margin Change Using LAG

WITH margin_analysis AS (
    SELECT
        company_name,
        year,
        profit_margin,
        LAG(profit_margin) OVER (
            PARTITION BY company_name
            ORDER BY year
        ) AS previous_margin
    FROM company_financials
    WHERE sales > 100
)
SELECT
    company_name,
    year,
    profit_margin,
    previous_margin,
    profit_margin - previous_margin AS margin_change
FROM margin_analysis
WHERE previous_margin IS NOT NULL
  AND profit_margin IS NOT NULL
ORDER BY margin_change DESC;


-- 5.6 Moving Average of Sales

SELECT
    company_name,
    year,
    sales,
    AVG(sales) OVER (
        PARTITION BY company_name
        ORDER BY year
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS three_year_avg_sales
FROM company_financials
WHERE sales IS NOT NULL
ORDER BY company_name, year;



-- 6.1 Highest EPS

SELECT
    company_name,
    eps_in_rs,
    net_profit,
    sales
FROM company_financials
WHERE year = 2023
  AND eps_in_rs IS NOT NULL
ORDER BY eps_in_rs DESC
LIMIT 10;


-- 6.2 Highest EPS Growth

SELECT
    company_name,
    eps_in_rs,
    eps_growth
FROM company_financials
WHERE year = 2023
  AND eps_growth IS NOT NULL
ORDER BY eps_growth DESC
LIMIT 10;


-- 6.3 Negative EPS Companies

SELECT
    company_name,
    eps_in_rs,
    net_profit
FROM company_financials
WHERE year = 2023
  AND eps_in_rs < 0
ORDER BY eps_in_rs ASC;




-- 7.1 Strong Companies

SELECT
    company_name,
    sales,
    revenue_growth,
    profit_growth,
    profit_margin,
    opm,
    interest_coverage,
    cagr
FROM company_financials
WHERE year = 2023
  AND sales > 1000
  AND revenue_growth > 10
  AND profit_growth > 10
  AND profit_margin > 10
  AND interest_coverage > 3
ORDER BY cagr DESC;


-- 7.2 High Sales but Low Profitability

SELECT
    company_name,
    sales,
    net_profit,
    profit_margin,
    opm
FROM company_financials
WHERE year = 2023
  AND sales > 1000
  AND profit_margin < 5
ORDER BY sales DESC;


-- 7.3 High Profitability + Strong Growth

SELECT
    company_name,
    sales,
    profit_margin,
    revenue_growth,
    profit_growth,
    cagr
FROM company_financials
WHERE year = 2023
  AND sales > 1000
  AND profit_margin > 10
  AND revenue_growth > 10
  AND profit_growth > 10
ORDER BY cagr DESC;




-- 🔥 8 advance sql — Financial Health Classification
-- Now combine CTE + CASE + financial metrics.

WITH financial_analysis AS (
	SELECT
        company_name,
        year,
        sales,
        revenue_growth,
        profit_growth,
        profit_margin,
        interest_coverage,
        cagr
    FROM company_financials
    WHERE year = 2023
)
SELECT
    company_name,
    sales,
    revenue_growth,
    profit_growth,
    profit_margin,
    interest_coverage,
    cagr,
    CASE
        WHEN profit_margin > 15
         AND revenue_growth > 10
         AND profit_growth > 10
         AND interest_coverage > 5
            THEN 'Strong'
       	WHEN profit_margin > 5
         AND revenue_growth > 0
         AND profit_growth > 0
         AND interest_coverage > 2
            THEN 'Healthy'
        WHEN profit_margin < 0
          OR interest_coverage < 1
          OR profit_growth < 0
            THEN 'Risk'
        ELSE 'Moderate'
    END AS financial_health
FROM financial_analysis;



