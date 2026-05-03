"""Great Expectations data quality checks for the analytics star schema."""
import os
import sys

import great_expectations as gx
import pandas as pd
import sqlalchemy

CONN = (
    f"postgresql+psycopg2://"
    f"{os.environ.get('ANALYTICS_USER', 'analytics_user')}:"
    f"{os.environ.get('ANALYTICS_PASSWORD', 'analytics_1234')}@"
    f"{os.environ.get('ANALYTICS_HOST', 'localhost')}:"
    f"{os.environ.get('ANALYTICS_PORT', '5433')}/"
    f"{os.environ.get('ANALYTICS_DB', 'analytics_db')}"
)


def load(table: str, limit: int = 100_000) -> pd.DataFrame:
    engine = sqlalchemy.create_engine(CONN)
    with engine.connect() as conn:
        return pd.read_sql(f"SELECT * FROM analytics.{table} LIMIT {limit}", conn)


def validate(table: str, df: pd.DataFrame, expectations: list) -> tuple[bool, list]:
    context = gx.get_context(mode="ephemeral")
    ds = context.sources.add_pandas(name=table)
    asset = ds.add_dataframe_asset(name=table)
    br = asset.build_batch_request(dataframe=df)
    suite = context.add_expectation_suite(expectation_suite_name=f"{table}_suite")
    validator = context.get_validator(
        batch_request=br,
        expectation_suite_name=f"{table}_suite",
    )
    for method, kwargs in expectations:
        getattr(validator, method)(**kwargs)

    result = validator.validate()
    failures = [
        f"{r['expectation_config']['expectation_type']}({r['expectation_config']['kwargs']})"
        for r in result.results
        if not r["success"]
    ]
    return result.success, failures


SUITES = {
    "dim_date": [
        # date_sk is the YYYYMMDD surrogate key — must be unique and never null
        ("expect_column_values_to_not_be_null",   {"column": "date_sk"}),
        ("expect_column_values_to_be_unique",      {"column": "date_sk"}),
    ],
    "dim_order_status": [
        # only the four known status codes should exist
        ("expect_column_values_to_be_in_set", {
            "column": "status_code",
            "value_set": ["PENDING", "PROCESSING", "REPROCESSING", "COMPLETED"],
        }),
        # every status row must have an explicit open/closed flag
        ("expect_column_values_to_not_be_null", {"column": "is_open"}),
    ],
    "dim_customer": [
        # natural key must always be present
        ("expect_column_values_to_not_be_null", {"column": "customer_id"}),
        # surrogate key must be globally unique (SCD2 integrity)
        ("expect_column_values_to_be_unique",   {"column": "customer_sk"}),
    ],
    "dim_product": [
        # natural key must always be present
        ("expect_column_values_to_not_be_null", {"column": "product_id"}),
        # price must be positive in >99% of rows — source RANDOM() can rarely produce 0
        ("expect_column_values_to_be_between", {
            "column": "unity_price",
            "min_value": 0,
            "strict_min": True,
            "mostly": 0.99,
        }),
    ],
    "fact_orders": [
        # one row per order — duplicates would double-count metrics
        ("expect_column_values_to_be_unique",   {"column": "order_id"}),
        # every fact row must resolve to a known status
        ("expect_column_values_to_not_be_null", {"column": "status_sk"}),
    ],
    "fact_order_items": [
        # one row per order line — duplicates inflate pending quantities
        ("expect_column_values_to_be_unique",   {"column": "order_item_id"}),
        # every item must belong to an order
        ("expect_column_values_to_not_be_null", {"column": "order_id"}),
    ],
}


def main():
    print("Great Expectations — Analytics DB quality checks\n")
    all_passed = True

    for table, expectations in SUITES.items():
        print(f"  {table}")
        df = load(table)
        passed, failures = validate(table, df, expectations)
        if passed:
            print(f"    ✓  {len(expectations)} checks passed  ({len(df):,} rows)\n")
        else:
            all_passed = False
            print(f"    ✗  FAILED:\n")
            for f in failures:
                print(f"      - {f}\n")

    if all_passed:
        print("All suites passed.")
    else:
        print("One or more suites failed.")

    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
