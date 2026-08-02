from pyspark.sql.functions import col, regexp_replace


def transform_data(df):
    """
    Perform all data cleaning and reshaping transformations.
    """

    # ==========================================================
    # 1. Rename column
    # ==========================================================
    df = df.withColumnRenamed("Unnamed: 0", "metric")

    # ==========================================================
    # 2. Select required columns
    # ==========================================================
    df = df.select(
        "metric",
        "company_name",
        "Mar 2012",
        "Mar 2013",
        "Mar 2014",
        "Mar 2015",
        "Mar 2016",
        "Mar 2017",
        "Mar 2018",
        "Mar 2019",
        "Mar 2020",
        "Mar 2021",
        "Mar 2022",
        "Mar 2023"
    )

    # ==========================================================
    # 3. Wide → Long (Unpivot)
    # ==========================================================
    df = df.selectExpr(
        "metric",
        "company_name",
        """
        stack(
            12,
            'Mar 2012', `Mar 2012`,
            'Mar 2013', `Mar 2013`,
            'Mar 2014', `Mar 2014`,
            'Mar 2015', `Mar 2015`,
            'Mar 2016', `Mar 2016`,
            'Mar 2017', `Mar 2017`,
            'Mar 2018', `Mar 2018`,
            'Mar 2019', `Mar 2019`,
            'Mar 2020', `Mar 2020`,
            'Mar 2021', `Mar 2021`,
            'Mar 2022', `Mar 2022`,
            'Mar 2023', `Mar 2023`
        ) as (year, value)
        """
    )

    # ==========================================================
    # 4. Convert Year
    # ==========================================================
    df = df.withColumn(
        "year",
        regexp_replace(col("year"), "Mar ", "").cast("int")
    )

    # ==========================================================
    # 5. Keep only required financial metrics
    # ==========================================================
    df = df.filter(
        col("metric").isin(
            "Sales",
            "Expenses",
            "Operating Profit",
            "Interest",
            "Depreciation",
            "Profit before tax",
            "Net Profit",
            "EPS in Rs"
        )
    )

    # ==========================================================
    # 6. Remove null values
    # ==========================================================
    df = df.dropna(subset=["value"])

    # ==========================================================
    # 7. Pivot
    # ==========================================================
    df = (
        df.groupBy("company_name", "year")
          .pivot("metric")
          .agg({"value": "first"})
    )

    # ==========================================================
    # 8. Arrange columns
    # ==========================================================
    df = df.select(
        "company_name",
        "year",
        "Sales",
        "Expenses",
        "Operating Profit",
        "Interest",
        "Depreciation",
        "Profit before tax",
        "Net Profit",
        "EPS in Rs"
    )

    # ==========================================================
    # 9. Arrange rows
    # ==========================================================
    df = df.orderBy("company_name", "year")

    # ==========================================================
    # 10. Standardize column names
    # ==========================================================
    new_columns = []

    for column in df.columns:
        new_columns.append(
            column.lower().replace(" ", "_")
        )

    df = df.toDF(*new_columns)


    return df