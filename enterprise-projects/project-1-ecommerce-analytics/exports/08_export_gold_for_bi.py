"""
Gold -> BI export. Dashboards (Power BI / Tableau) don't read Delta well
out of the box, so the last mile is a plain Parquet + CSV drop of each
Gold table - CSV for anyone who just wants to open it in Excel, Parquet
for the BI tool's native connector.

Run with: python 08_export_gold_for_bi.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import get_spark, GOLD_PATH, BASE_PATH

EXPORT_TABLES = ["customer_360", "daily_category_rollup", "revenue_leakage_daily", "customer_segments_kmeans"]


def export_table(spark, table_name):
    df = spark.read.format("delta").load(f"{GOLD_PATH}/{table_name}")
    export_root = f"{BASE_PATH}/exports/{table_name}"

    df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{export_root}_csv")
    df.write.mode("overwrite").parquet(f"{export_root}_parquet")
    print(f"  {table_name}: {df.count():,} rows -> {export_root}_{{csv,parquet}}")


if __name__ == "__main__":
    spark = get_spark("EcomProject_ExportForBI")

    print("Exporting gold tables for Power BI / Tableau:")
    for table in EXPORT_TABLES:
        export_table(spark, table)

    spark.stop()
