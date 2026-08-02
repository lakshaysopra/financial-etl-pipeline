from pyspark.sql.functions import (
    col,
    when,
    round,
    lag,
    row_number
)

from pyspark.sql.window import Window

from utils.logger import logger


def calculate_kpis(df):

    """
    Calculate financial KPIs from transformed financial data.
    """

    logger.info("=" * 60)
    logger.info("STARTING KPI CALCULATIONS")
    logger.info("=" * 60)

    # ==========================================================
    # 1. PROFITABILITY KPIs
    # ==========================================================

    # Profit Margin
    df = df.withColumn(
        "profit_margin",
        when(
            col("sales") != 0,
            round(
                (col("net_profit") / col("sales")) * 100,
                2
            )
        )
    )

    # Operating Profit Margin
    df = df.withColumn(
        "opm",
        when(
            col("sales") != 0,
            round(
                (col("operating_profit") / col("sales")) * 100,
                2
            )
        )
    )

    # Profit Before Tax Margin
    df = df.withColumn(
        "pbt_margin",
        when(
            col("sales") != 0,
            round(
                (col("profit_before_tax") / col("sales")) * 100,
                2
            )
        )
    )

    # Expense Ratio
    df = df.withColumn(
        "expense_ratio",
        when(
            col("sales") != 0,
            round(
                (col("expenses") / col("sales")) * 100,
                2
            )
        )
    )

    # ==========================================================
    # 2. INTEREST KPIs
    # ==========================================================

    # Interest Coverage
    df = df.withColumn(
        "interest_coverage",
        when(
            col("interest") != 0,
            round(
                col("operating_profit") / col("interest"),
                2
            )
        )
    )

    # Interest Burden Ratio
    df = df.withColumn(
        "interest_burden_ratio",
        when(
            col("sales") != 0,
            round(
                (col("interest") / col("sales")) * 100,
                2
            )
        )
    )

    # ==========================================================
    # 3. TAX KPIs
    # ==========================================================

    # Tax Burden Ratio
    df = df.withColumn(
        "tax_burden_ratio",
        when(
            col("profit_before_tax") != 0,
            round(
                (
                    col("net_profit")
                    / col("profit_before_tax")
                ) * 100,
                2
            )
        )
    )

    # Effective Tax Rate
    df = df.withColumn(
        "effective_tax_rate",
        when(
            col("profit_before_tax") != 0,
            round(
                (
                    (
                        col("profit_before_tax")
                        - col("net_profit")
                    )
                    / col("profit_before_tax")
                ) * 100,
                2
            )
        )
    )

    # ==========================================================
    # 4. WINDOW FOR YoY CALCULATIONS
    # ==========================================================

    company_window = (
        Window
        .partitionBy("company_name")
        .orderBy("year")
    )

    # ==========================================================
    # 5. REVENUE GROWTH
    # ==========================================================

    df = df.withColumn(
        "previous_sales",
        lag("sales").over(company_window)
    )

    df = df.withColumn(
        "revenue_growth",
        when(
            col("previous_sales") != 0,
            round(
                (
                    (
                        col("sales")
                        - col("previous_sales")
                    )
                    / col("previous_sales")
                ) * 100,
                2
            )
        )
    )

    df = df.drop("previous_sales")

    # ==========================================================
    # 6. PROFIT GROWTH
    # ==========================================================

    df = df.withColumn(
        "previous_profit",
        lag("net_profit").over(company_window)
    )

    df = df.withColumn(
        "profit_growth",
        when(
            col("previous_profit") != 0,
            round(
                (
                    (
                        col("net_profit")
                        - col("previous_profit")
                    )
                    / col("previous_profit")
                ) * 100,
                2
            )
        )
    )

    df = df.drop("previous_profit")

    # ==========================================================
    # 7. EPS GROWTH
    # ==========================================================

    df = df.withColumn(
        "previous_eps",
        lag("eps_in_rs").over(company_window)
    )

    df = df.withColumn(
        "eps_growth",
        when(
            col("previous_eps") != 0,
            round(
                (
                    (
                        col("eps_in_rs")
                        - col("previous_eps")
                    )
                    / col("previous_eps")
                ) * 100,
                2
            )
        )
    )

    df = df.drop("previous_eps")

    # ==========================================================
    # 8. CAGR
    # ==========================================================

    start_window = (
        Window
        .partitionBy("company_name")
        .orderBy("year")
    )

    end_window = (
        Window
        .partitionBy("company_name")
        .orderBy(col("year").desc())
    )

    cagr_base = (
        df
        .withColumn(
            "start_rank",
            row_number().over(start_window)
        )
        .withColumn(
            "end_rank",
            row_number().over(end_window)
        )
    )

    # Beginning sales
    start_df = (
        cagr_base
        .filter(col("start_rank") == 1)
        .select(
            "company_name",
            col("year").alias("start_year"),
            col("sales").alias("beginning_sales")
        )
    )

    # Ending sales
    end_df = (
        cagr_base
        .filter(col("end_rank") == 1)
        .select(
            "company_name",
            col("year").alias("end_year"),
            col("sales").alias("ending_sales")
        )
    )

    # Join beginning and ending values
    cagr_df = start_df.join(
        end_df,
        on="company_name",
        how="inner"
    )

    # Number of years
    cagr_df = cagr_df.withColumn(
        "years",
        col("end_year") - col("start_year")
    )

    # CAGR calculation
    cagr_df = cagr_df.withColumn(
        "cagr",
        when(
            (col("beginning_sales") > 0)
            & (col("ending_sales") >= 0)
            & (col("years") > 0),
            round(
                (
                    (
                        col("ending_sales")
                        / col("beginning_sales")
                    ) ** (1 / col("years"))
                    - 1
                ) * 100,
                4
            )
        )
    )

    cagr_df = cagr_df.select(
        "company_name",
        "cagr"
    )

    # Add CAGR to main DataFrame
    df = df.join(
        cagr_df,
        on="company_name",
        how="left"
    )

    # ==========================================================
    # 9. DEPRECIATION RATIO
    # ==========================================================

    df = df.withColumn(
        "depreciation_ratio",
        when(
            col("sales") != 0,
            round(
                (
                    col("depreciation")
                    / col("sales")
                ) * 100,
                2
            )
        )
    )

    # ==========================================================
    # 10. OPERATING LEVERAGE
    # ==========================================================

    df = df.withColumn(
        "previous_operating_profit",
        lag("operating_profit").over(company_window)
    )

    df = df.withColumn(
        "operating_profit_growth",
        when(
            col("previous_operating_profit") != 0,
            (
                (
                    col("operating_profit")
                    - col("previous_operating_profit")
                )
                / col("previous_operating_profit")
            ) * 100
        )
    )

    df = df.withColumn(
        "operating_leverage",
        when(
            col("revenue_growth") != 0,
            round(
                col("operating_profit_growth")
                / col("revenue_growth"),
                2
            )
        )
    )

    # Remove temporary columns
    df = df.drop(
        "previous_operating_profit",
        "operating_profit_growth"
    )

    logger.info("KPI calculations completed successfully.")

    return df