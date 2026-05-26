CREATE TABLE IF NOT EXISTS company_financials (
       id SERIAL PRIMARY KEY,
	   company_name VARCHAR(255),
	   financial_year INT,
	   sales NUMERIC(15,2),

      expenses NUMERIC(15,2),

    operating_profit NUMERIC(15,2),

    interest NUMERIC(15,2),

    depreciation NUMERIC(15,2),

    profit_before_tax NUMERIC(15,2),

    net_profit NUMERIC(15,2),

    eps_in_rs NUMERIC(10,2),

    profit_margin NUMERIC(10,2),

    opm NUMERIC(10,2),

    expense_ratio NUMERIC(10,2),

    interest_coverage NUMERIC(10,2),

    interest_burden_ratio NUMERIC(10,2),

    tax_burden_ratio NUMERIC(10,2),

    revenue_growth NUMERIC(10,2),

    profit_growth NUMERIC(10,2),

    cagr NUMERIC(10,2)

);