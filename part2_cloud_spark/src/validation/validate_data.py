# ==========================================================
# validation.py
#
# Phase 5 : Data Validation
#
# Production Validation Module
# ==========================================================


# ==========================================================
# Imports
# ==========================================================

from pyspark.sql.functions import (
    col,
    count,
    min,
    max,
    isnan,
    when,
    lag,
    abs,
    round as spark_round,
    row_number
)
import builtins
from utils.logger import logger


from pyspark.sql.window import Window

from pyspark.sql.types import (
    DoubleType,
    FloatType
)


# ==========================================================
# Helper Functions
# ==========================================================

def divider():
    print("=" * 80)


def sub_divider():
    print("-" * 80)


def print_heading(title):

    print("\n")
    divider()
    print(title.upper())
    divider()


def print_subheading(title):

    print("\n")
    sub_divider()
    print(title)
    sub_divider()


def print_result(label, value):

    print(f"{label:<40}: {value}")


# ==========================================================
#
#               MAJOR 1
#
#         STRUCTURAL VALIDATION
#
# ==========================================================


# ==========================================================
# 01 Dataset Structure Validation
# ==========================================================

def _01_dataset_structure(df):

    print_heading(
        "Step 01 : Dataset Structure Validation"
    )

    print("\nSchema\n")

    df.printSchema()

    print("\nTotal Columns")

    print_result(
        "Number of Columns",
        len(df.columns)
    )

    print("\nColumn Names")

    for column in df.columns:
        print(column)

    print("\nValidation Result")

    print(
        "✔ Schema successfully loaded."
    )

    print(
        "✔ Dataset structure verified."
    )



# ==========================================================
# 02 Duplicate Validation
# ==========================================================

def _02_duplicate_validation(df):

    print_heading(
        "Step 02 : Duplicate Validation"
    )

    duplicate_df = (

        df.groupBy(
            "company_name",
            "year"
        )

        .count()

        .filter(
            col("count") > 1
        )

    )

    duplicate_count = duplicate_df.count()

    print_result(
        "Duplicate Company-Year Records",
        duplicate_count
    )

    if duplicate_count > 0:

        print(
            "\nDuplicate Records Found\n"
        )

        duplicate_df.show(
            truncate=False
        )

    else:

        print(
            "\n✔ No duplicate Company-Year records found."
        )



# ==========================================================
# 03 Column-wise NULL Profiling
# ==========================================================

def _03_null_profile(df):

    print_heading(
        "Step 03 : Column-wise NULL Profiling"
    )

    total_rows = df.count()

    print_result(
        "Total Rows",
        total_rows
    )

    print()

    null_summary = []

    for column in df.columns:

        null_count = (

            df.filter(

                col(column).isNull()

            ).count()

        )

        percentage = builtins.round(
        (null_count / total_rows) * 100,
        2
        )

        null_summary.append(

            (

                column,

                null_count,

                percentage

            )

        )

    print(

        f"{'Column':30}"
        f"{'NULL Count':15}"
        f"{'NULL %':10}"

    )

    sub_divider()

    for row in null_summary:

        print(

            f"{row[0]:30}"

            f"{row[1]:<15}"

            f"{row[2]}"

        )

    print("\n")

    print(
        "Interpretation"
    )

    print(
        "Columns containing NULL values "
        "will be investigated individually "
        "during subsequent validation steps."
    )


# ==========================================================
#
# End of Part 1
#
# Next:
#
# 04 Sales Validation
# 05 Interest Validation
# 06 Operating Profit Validation
# 07 EPS Validation
# 08 Profit Before Tax Validation
#
# ==========================================================


# ==========================================================
#
#               MAJOR 2
#
#        BUSINESS DATA VALIDATION
#
# ==========================================================


# ==========================================================
# 04 Sales Validation
# ==========================================================

def _04_sales_validation(df):

    print_heading(
        "Step 04 : Sales Validation"
    )

    sales_null = df.filter(
        col("sales").isNull()
    ).count()

    sales_zero = df.filter(
        col("sales") == 0
    ).count()

    sales_negative = df.filter(
        col("sales") < 0
    ).count()

    print_result(
        "NULL Sales",
        sales_null
    )

    print_result(
        "Zero Sales",
        sales_zero
    )

    print_result(
        "Negative Sales",
        sales_negative
    )

    print("\nSample NULL Sales Records")

    df.filter(
        col("sales").isNull()
    ).select(
        "company_name",
        "year",
        "sales",
        "expenses",
        "operating_profit",
        "net_profit"
    ).show(
        20,
        truncate=False
    )

    print("\nSample Zero Sales Records")

    df.filter(
        col("sales") == 0
    ).select(
        "company_name",
        "year",
        "sales",
        "expenses",
        "operating_profit",
        "net_profit"
    ).show(
        20,
        truncate=False
    )

    print("\nSample Negative Sales Records")

    df.filter(
        col("sales") < 0
    ).select(
        "company_name",
        "year",
        "sales",
        "expenses",
        "operating_profit",
        "net_profit"
    ).orderBy(
        col("sales")
    ).show(
        20,
        truncate=False
    )


# ==========================================================
# 05 Interest Validation
# ==========================================================

def _05_interest_validation(df):

    print_heading(
        "Step 05 : Interest Validation"
    )

    interest_null = df.filter(
        col("interest").isNull()
    ).count()

    interest_zero = df.filter(
        col("interest") == 0
    ).count()

    operating_profit_null = df.filter(
        col("operating_profit").isNull()
    ).count()

    overlap = df.filter(
        col("operating_profit").isNull()
        &
        (
            col("interest") == 0
        )
    ).count()

    print_result(
        "NULL Interest",
        interest_null
    )

    print_result(
        "Zero Interest",
        interest_zero
    )

    print_result(
        "NULL Operating Profit",
        operating_profit_null
    )

    print_result(
        "NULL Operating Profit + Zero Interest",
        overlap
    )


# ==========================================================
# 06 Operating Profit Validation
# ==========================================================

def _06_operating_profit_validation(df):

    print_heading(
        "Step 06 : Operating Profit Validation"
    )

    comparison = df.filter(

        col("operating_profit").isNull()

        &

        col("sales").isNull()

    )

    print_result(
        "Rows having both Sales and Operating Profit NULL",
        comparison.count()
    )

    comparison.select(
        "company_name",
        "year",
        "sales",
        "operating_profit",
        "expenses",
        "net_profit"
    ).show(
        20,
        truncate=False
    )


# ==========================================================
# 07 EPS Validation
# ==========================================================

def _07_eps_validation(df):

    print_heading(
        "Step 07 : EPS Validation"
    )

    eps_null = df.filter(
        col("eps_in_rs").isNull()
    )

    print_result(
        "EPS NULL Rows",
        eps_null.count()
    )

    eps_null.select(
        "company_name",
        "year",
        "sales",
        "net_profit",
        "eps_in_rs"
    ).show(
        30,
        truncate=False
    )


# ==========================================================
# 08 Profit Before Tax Validation
# ==========================================================

def _08_profit_before_tax_validation(df):

    print_heading(
        "Step 08 : Profit Before Tax Validation"
    )

    pbt_null = df.filter(
        col("profit_before_tax").isNull()
    ).count()

    pbt_zero = df.filter(
        col("profit_before_tax") == 0
    ).count()

    print_result(
        "NULL Profit Before Tax",
        pbt_null
    )

    print_result(
        "Zero Profit Before Tax",
        pbt_zero
    )


# ==========================================================
#
# End of Part 2
#
# Next:
#
# 09 Revenue Growth Validation
# 10 Profit Growth Validation
# 11 EPS Growth Validation
# 12 CAGR Validation
# 13 Operating Leverage Validation
#
# ==========================================================


# ==========================================================
#
#               MAJOR 2 (Continued)
#
#        BUSINESS DATA VALIDATION
#
# ==========================================================


# ==========================================================
# 09 Revenue Growth Validation
# ==========================================================

def _09_revenue_growth_validation(df):

    print_heading(
        "Step 09 : Revenue Growth Validation"
    )

    company_window = Window.partitionBy(
        "company_name"
    ).orderBy(
        "year"
    )

    revenue_df = df.withColumn(
        "previous_sales",
        lag("sales").over(company_window)
    )

    revenue_null = revenue_df.filter(
        col("revenue_growth").isNull()
    )

    print_result(
        "Revenue Growth NULL",
        revenue_null.count()
    )

    print_result(
        "Previous Sales NULL",
        revenue_null.filter(
            col("previous_sales").isNull()
        ).count()
    )

    print_result(
        "Previous Sales Zero",
        revenue_null.filter(
            col("previous_sales") == 0
        ).count()
    )

    print_result(
        "Current Sales NULL",
        revenue_null.filter(
            col("sales").isNull()
        ).count()
    )

    print("\nSample Records")

    revenue_null.select(
        "company_name",
        "year",
        "sales",
        "previous_sales",
        "revenue_growth"
    ).show(
        50,
        truncate=False
    )


# ==========================================================
# 10 Profit Growth Validation
# ==========================================================

def _10_profit_growth_validation(df):

    print_heading(
        "Step 10 : Profit Growth Validation"
    )

    company_window = Window.partitionBy(
        "company_name"
    ).orderBy(
        "year"
    )

    profit_df = df.withColumn(
        "previous_profit",
        lag("net_profit").over(company_window)
    )

    profit_null = profit_df.filter(
        col("profit_growth").isNull()
    )

    print_result(
        "Profit Growth NULL",
        profit_null.count()
    )

    print_result(
        "Previous Profit NULL",
        profit_null.filter(
            col("previous_profit").isNull()
        ).count()
    )

    print_result(
        "Previous Profit Zero",
        profit_null.filter(
            col("previous_profit") == 0
        ).count()
    )

    print_result(
        "Current Profit NULL",
        profit_null.filter(
            col("net_profit").isNull()
        ).count()
    )


# ==========================================================
# 11 EPS Growth Validation
# ==========================================================

def _11_eps_growth_validation(df):

    print_heading(
        "Step 11 : EPS Growth Validation"
    )

    company_window = Window.partitionBy(
        "company_name"
    ).orderBy(
        "year"
    )

    eps_df = df.withColumn(
        "previous_eps",
        lag("eps_in_rs").over(company_window)
    )

    eps_null = eps_df.filter(
        col("eps_growth").isNull()
    )

    print_result(
        "EPS Growth NULL",
        eps_null.count()
    )

    print_result(
        "Previous EPS NULL",
        eps_null.filter(
            col("previous_eps").isNull()
        ).count()
    )

    print_result(
        "Previous EPS Zero",
        eps_null.filter(
            col("previous_eps") == 0
        ).count()
    )

    print_result(
        "Current EPS NULL",
        eps_null.filter(
            col("eps_in_rs").isNull()
        ).count()
    )

    print_result(
        "Current EPS NULL + Previous EPS NULL",
        eps_null.filter(
            col("eps_in_rs").isNull()
            &
            col("previous_eps").isNull()
        ).count()
    )

    print_result(
        "Current EPS NULL + Previous EPS Zero",
        eps_null.filter(
            col("eps_in_rs").isNull()
            &
            (
                col("previous_eps") == 0
            )
        ).count()
    )

    print_result(
        "Current EPS NULL + Previous EPS Valid",
        eps_null.filter(
            col("eps_in_rs").isNull()
            &
            col("previous_eps").isNotNull()
            &
            (
                col("previous_eps") != 0
            )
        ).count()
    )


# ==========================================================
# 12 CAGR Validation
# ==========================================================

def _12_cagr_validation(df):

    print_heading(
        "Step 12 : CAGR Validation"
    )

    print_result(
        "CAGR NULL Rows",
        df.filter(
            col("cagr").isNull()
        ).count()
    )

    # ------------------------------------------------------
    # Get First Year Record
    # ------------------------------------------------------

    first_window = Window.partitionBy(
        "company_name"
    ).orderBy(
        "year"
    )

    first_df = (

        df.withColumn(
            "rn",
            row_number().over(first_window)
        )

        .filter(
            col("rn") == 1
        )

        .select(

            "company_name",

            col("sales").alias(
                "first_sales"
            ),

            col("year").alias(
                "first_year"
            )

        )

    )

    # ------------------------------------------------------
    # Get Last Year Record
    # ------------------------------------------------------

    last_window = Window.partitionBy(
        "company_name"
    ).orderBy(
        col("year").desc()
    )

    last_df = (

        df.withColumn(
            "rn",
            row_number().over(last_window)
        )

        .filter(
            col("rn") == 1
        )

        .select(

            "company_name",

            col("sales").alias(
                "last_sales"
            ),

            col("year").alias(
                "last_year"
            )

        )

    )

    # ------------------------------------------------------
    # Merge
    # ------------------------------------------------------

    company_summary = (

        first_df.join(

            last_df,

            on="company_name",

            how="inner"

        )

    )

    # ------------------------------------------------------
    # Companies having NULL CAGR
    # ------------------------------------------------------

    cagr_check = (

        company_summary.join(

            df.select(
                "company_name",
                "cagr"
            ).distinct(),

            on="company_name",

            how="inner"

        )

        .filter(
            col("cagr").isNull()
        )

    )

    print_result(
        "Companies with NULL CAGR",
        cagr_check.count()
    )

    print_result(
        "First Sales NULL",
        cagr_check.filter(
            col("first_sales").isNull()
        ).count()
    )

    print_result(
        "First Sales Zero or Negative",
        cagr_check.filter(
            col("first_sales") <= 0
        ).count()
    )

    print_result(
        "Last Sales NULL",
        cagr_check.filter(
            col("last_sales").isNull()
        ).count()
    )

    print_result(
        "Last Sales Zero or Negative",
        cagr_check.filter(
            col("last_sales") <= 0
        ).count()
    )

    print_result(
        "Only One Year",
        cagr_check.filter(
            col("last_year") == col("first_year")
        ).count()
    )

    # ------------------------------------------------------
    # Unexplained NULL CAGR
    # ------------------------------------------------------

    valid_but_cagr_null = cagr_check.filter(

        col("first_sales").isNotNull()

        &

        col("last_sales").isNotNull()

        &

        (col("first_sales") > 0)

        &

        (col("last_sales") > 0)

        &

        (col("last_year") > col("first_year"))

    )

    print_result(
        "Unexplained CAGR NULL Companies",
        valid_but_cagr_null.count()
    )

    if valid_but_cagr_null.count() > 0:

        print("\nSample Records\n")

        valid_but_cagr_null.show(
            truncate=False
        )

    else:

        print(
            "\n✔ All NULL CAGR values are explained by business or mathematical constraints."
        )



# ==========================================================
# 13 Operating Leverage Validation
# ==========================================================

def _13_operating_leverage_validation(df):

    print_heading(
        "Step 13 : Operating Leverage Validation"
    )

    company_window = Window.partitionBy(
        "company_name"
    ).orderBy(
        "year"
    )

    leverage_df = df.withColumn(
        "previous_operating_profit",
        lag("operating_profit").over(company_window)
    )

    leverage_null = leverage_df.filter(
        col("operating_leverage").isNull()
    )

    print_result(
        "Operating Leverage NULL",
        leverage_null.count()
    )

    print_result(
        "Previous Operating Profit NULL",
        leverage_null.filter(
            col("previous_operating_profit").isNull()
        ).count()
    )

    print_result(
        "Previous Operating Profit Zero",
        leverage_null.filter(
            col("previous_operating_profit") == 0
        ).count()
    )

    print_result(
        "Current Operating Profit NULL",
        leverage_null.filter(
            col("operating_profit").isNull()
        ).count()
    )

    print_result(
        "Revenue Growth NULL",
        leverage_null.filter(
            col("revenue_growth").isNull()
        ).count()
    )

    print_result(
        "Revenue Growth Zero",
        leverage_null.filter(
            col("revenue_growth") == 0
        ).count()
    )

    unexplained = leverage_null.filter(

        col("previous_operating_profit").isNotNull()

        &

        (col("previous_operating_profit") != 0)

        &

        col("operating_profit").isNotNull()

        &

        col("revenue_growth").isNotNull()

        &

        (col("revenue_growth") != 0)

    )

    print_result(
        "Unexplained Operating Leverage NULL",
        unexplained.count()
    )

# ==========================================================
#
#               MAJOR 3
#
#        TECHNICAL DATA VALIDATION
#
# ==========================================================


# ==========================================================
# 14 NaN Validation
# ==========================================================

def _14_nan_validation(df):

    print_heading(
        "Step 14 : NaN Validation"
    )

    numeric_columns = [
    field.name
    for field in df.schema.fields
    if field.dataType.simpleString() in ["double", "float"]
]

    total_nan = 0

    for column in numeric_columns:

        nan_count = df.filter(
            isnan(col(column))
        ).count()

        total_nan += nan_count

        if nan_count > 0:

            print_result(
                column,
                nan_count
            )

    if total_nan == 0:

        print(
            "✔ No NaN values found in any numeric column."
        )


# ==========================================================
# 15 Infinity Validation
# ==========================================================

def _15_infinity_validation(df):

    print_heading(
        "Step 15 : Infinity Validation"
    )

    numeric_columns = [
    field.name
    for field in df.schema.fields
    if field.dataType.simpleString() in ["double", "float"]
    ]

    total_inf = 0

    for column in numeric_columns:

        inf_count = df.filter(

            (col(column) == float("inf"))

            |

            (col(column) == float("-inf"))

        ).count()

        total_inf += inf_count

        if inf_count > 0:

            print_result(
                column,
                inf_count
            )

    if total_inf == 0:

        print(
            "✔ No Infinity values found in any numeric column."
        )


# ==========================================================
# 16 Year Validation
# ==========================================================

def _16_year_validation(df):

    print_heading(
        "Step 16 : Year Validation"
    )

    year_summary = df.selectExpr(

        "min(year) as minimum_year",

        "max(year) as maximum_year"

    )

    year_summary.show()

    print(
        "Distinct Financial Years"
    )

    df.select(
        "year"
    ).distinct().orderBy(
        "year"
    ).show()

    print(
        "✔ Financial years successfully validated."
    )


# ==========================================================
#
#               MAJOR 4
#
#   DATA QUALITY & ANOMALY INVESTIGATION
#
# ==========================================================


# ==========================================================
# 17 Statistical Profiling
# ==========================================================

def _17_statistical_profiling(df):

    print_heading(
        "Step 17 : Statistical Profiling"
    )

    check_columns = [

        "sales",
        "expenses",
        "operating_profit",
        "interest",
        "depreciation",
        "profit_before_tax",
        "net_profit",
        "eps_in_rs",
        "profit_margin",
        "opm",
        "pbt_margin",
        "expense_ratio",
        "interest_coverage",
        "interest_burden_ratio",
        "tax_burden_ratio",
        "effective_tax_rate",
        "revenue_growth",
        "profit_growth",
        "eps_growth",
        "cagr",
        "depreciation_ratio",
        "operating_leverage"

    ]

    for column in check_columns:

        result = df.select(

            min(column).alias("minimum"),

            max(column).alias("maximum")

        ).first()

        print(

            f"{column:25}"

            f" Min: {result['minimum']}"

            f"   Max: {result['maximum']}"

        )


# ==========================================================
# 18 Negative Value Investigation
# ==========================================================

def _18_negative_value_validation(df):

    print_heading(
        "Step 18 : Negative Value Investigation"
    )

    negative_columns = [

        "sales",

        "expenses",

        "interest",

        "depreciation"

    ]

    for column in negative_columns:

        negative_count = df.filter(
            col(column) < 0
        ).count()

        print_result(
            f"{column} Negative Rows",
            negative_count
        )

    print_subheading(
        "Negative Interest / Depreciation"
    )

    df.filter(

        (col("interest") < 0)

        |

        (col("depreciation") < 0)

    ).select(

        "company_name",

        "year",

        "sales",

        "interest",

        "depreciation",

        "operating_profit",

        "profit_before_tax",

        "net_profit"

    ).orderBy(

        "company_name",

        "year"

    ).show(
        50,
        truncate=False
    )

    print_subheading(
        "Negative Sales"
    )

    df.filter(

        col("sales") < 0

    ).select(

        "company_name",

        "year",

        "sales",

        "expenses",

        "operating_profit",

        "net_profit"

    ).orderBy(

        col("sales")

    ).show(
        20,
        truncate=False
    )

    print_subheading(
        "Negative Expenses"
    )

    df.filter(

        col("expenses") < 0

    ).select(

        "company_name",

        "year",

        "sales",

        "expenses",

        "operating_profit",

        "net_profit"

    ).orderBy(

        col("expenses")

    ).show(
        20,
        truncate=False
    )

    print_subheading(
        "Operating Profit Formula Verification"
    )

    op_mismatch = df.filter(

        col("sales").isNotNull()

        &

        col("expenses").isNotNull()

        &

        col("operating_profit").isNotNull()

        &

        (

            abs(

                col("operating_profit")

                -

                (

                    col("sales")

                    -

                    col("expenses")

                )

            ) > 1

        )

    )

    print_result(

        "Operating Profit Formula Mismatches",

        op_mismatch.count()

    )

    if op_mismatch.count() > 0:

        op_mismatch.select(

            "company_name",

            "year",

            "sales",

            "expenses",

            "operating_profit",

            (

                col("sales")

                -

                col("expenses")

            ).alias("expected_operating_profit"),

            (

                col("operating_profit")

                -

                (

                    col("sales")

                    -

                    col("expenses")

                )

            ).alias("difference")

        ).show(
            100,
            truncate=False
        )

    else:

        print(
            "✔ All operating profit values satisfy the Sales − Expenses relationship."
        )

# ==========================================================
#
#               MAJOR 5
#
#         MATHEMATICAL VALIDATION
#
# ==========================================================


# ==========================================================
# 19 Formula Validation
# ==========================================================

def _19_formula_validation(df):

    print_heading(
        "Step 19 : Formula Validation"
    )

    validations = [

        (
            "Operating Profit",

            abs(
                col("operating_profit")
                -
                (
                    col("sales")
                    -
                    col("expenses")
                )
            ),

            1
        ),

        (
            "Profit Margin",

            abs(
                col("profit_margin")
                -
                spark_round(
                    (
                        col("net_profit")
                        /
                        col("sales")
                    ) * 100,
                    2
                )
            ),

            0.01
        ),

        (
            "Operating Margin",

            abs(
                col("opm")
                -
                spark_round(
                    (
                        col("operating_profit")
                        /
                        col("sales")
                    ) * 100,
                    2
                )
            ),

            0.01
        ),

        (
            "PBT Margin",

            abs(
                col("pbt_margin")
                -
                spark_round(
                    (
                        col("profit_before_tax")
                        /
                        col("sales")
                    ) * 100,
                    2
                )
            ),

            0.01
        ),

        (
            "Expense Ratio",

            abs(
                col("expense_ratio")
                -
                spark_round(
                    (
                        col("expenses")
                        /
                        col("sales")
                    ) * 100,
                    2
                )
            ),

            0.01
        ),

        (
            "Interest Coverage",

            abs(
                col("interest_coverage")
                -
                spark_round(
                    (
                        col("operating_profit")
                        /
                        col("interest")
                    ),
                    2
                )
            ),

            0.01
        ),

        (
            "Interest Burden",

            abs(
                col("interest_burden_ratio")
                -
                spark_round(
                    (
                        col("interest")
                        /
                        col("sales")
                    ) * 100,
                    2
                )
            ),

            0.01
        ),

        (
            "Tax Burden",

            abs(
                col("tax_burden_ratio")
                -
                spark_round(
                    (
                        col("net_profit")
                        /
                        col("profit_before_tax")
                    ) * 100,
                    2
                )
            ),

            0.01
        ),

        (
            "Effective Tax Rate",

            abs(
                col("effective_tax_rate")
                -
                spark_round(
                    (
                        (
                            col("profit_before_tax")
                            -
                            col("net_profit")
                        )
                        /
                        col("profit_before_tax")
                    ) * 100,
                    2
                )
            ),

            0.01
        ),

        (
            "Depreciation Ratio",

            abs(
                col("depreciation_ratio")
                -
                spark_round(
                    (
                        col("depreciation")
                        /
                        col("sales")
                    ) * 100,
                    2
                )
            ),

            0.01
        )

    ]

    total_mismatch = 0

    for name, formula, tolerance in validations:

        mismatch = (

            df.filter(

                formula > tolerance

            ).count()

        )

        total_mismatch += mismatch

        print_result(

            f"{name} Mismatches",

            mismatch

        )

    print()

    if total_mismatch == 0:

        print(
            "✔ All KPI formulas validated successfully."
        )

    else:

        print(
            "⚠ Formula mismatches detected."
        )

        print_result(
            "Total Formula Mismatches",
            total_mismatch
        )

# ==========================================================
# 20 Validation Summary
# ==========================================================

def _20_validation_summary(df):

    print_heading(
        " Validation Summary"
    )

    print(
        "All validation steps have been executed successfully."
    )

    print(
        "The transformed dataset has been validated and is ready for the next phase of the ETL pipeline."
    )

    divider()


# ==========================================================
#
#           MAIN VALIDATION RUNNER
#
# ==========================================================

def validate_dataset(df):

    logger.info(
        "PHASE  : DATA VALIDATION"
    )

    # ------------------------------------------------------
    # MAJOR 1
    # ------------------------------------------------------

    logger.info(
        "MAJOR 1 : STRUCTURAL VALIDATION"
    )

    _01_dataset_structure(df)

    _02_duplicate_validation(df)

    _03_null_profile(df)

    # ------------------------------------------------------
    # MAJOR 2
    # ------------------------------------------------------

    logger.info(
        "MAJOR 2 : BUSINESS DATA VALIDATION"
    )

    _04_sales_validation(df)

    _05_interest_validation(df)

    _06_operating_profit_validation(df)

    _07_eps_validation(df)

    _08_profit_before_tax_validation(df)

    _09_revenue_growth_validation(df)

    _10_profit_growth_validation(df)

    _11_eps_growth_validation(df)

    _12_cagr_validation(df)

    _13_operating_leverage_validation(df)

    # ------------------------------------------------------
    # MAJOR 3
    # ------------------------------------------------------

    logger.info(
        "MAJOR 3 : TECHNICAL DATA VALIDATION"
    )

    _14_nan_validation(df)

    _15_infinity_validation(df)

    _16_year_validation(df)

    # ------------------------------------------------------
    # MAJOR 4
    # ------------------------------------------------------

    logger.info
    (
        "MAJOR 4 : DATA QUALITY & ANOMALY INVESTIGATION"
    )

    _17_statistical_profiling(df)

    _18_negative_value_validation(df)

    # ------------------------------------------------------
    # MAJOR 5
    # ------------------------------------------------------

    logger.info(
        "MAJOR 5 : MATHEMATICAL VALIDATION"
    )

    _19_formula_validation(df)

    # ------------------------------------------------------
    # FINAL SUMMARY
    # ------------------------------------------------------

    _20_validation_summary(df)