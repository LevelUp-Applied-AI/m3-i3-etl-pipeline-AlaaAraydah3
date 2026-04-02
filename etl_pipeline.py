"""ETL Pipeline — Amman Digital Market Customer Analytics

Extracts data from PostgreSQL, transforms it into customer-level summaries,
validates data quality, and loads results to a database table and CSV file.
"""
from sqlalchemy import create_engine
import pandas as pd
import os


def extract(engine):
    customers = pd.read_sql("SELECT * FROM customers", engine)
    products = pd.read_sql("SELECT * FROM products", engine)
    orders = pd.read_sql("SELECT * FROM orders", engine)
    order_items = pd.read_sql("SELECT * FROM order_items", engine)

    return {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items
    }


def transform(data_dict):
    customers = data_dict["customers"]
    products = data_dict["products"]
    orders = data_dict["orders"]
    order_items = data_dict["order_items"]

    # Fix dtype mismatch
    customers["customer_id"] = customers["customer_id"].astype("float")
    orders["customer_id"] = orders["customer_id"].astype("float")

    # Merge tables
    df = orders.merge(order_items, on="order_id")
    df = df.merge(products, on="product_id")
    df = df.merge(customers, on="customer_id")

    # Fix column names after merge
    df = df.rename(columns={
        "name_x": "product_name",
        "name_y": "customer_name"
    })

    # Filters
    df = df[df["status"] != "cancelled"]
    df = df[df["quantity"] <= 100]

    # Handle empty case (important for tests)
    if df.empty:
        return pd.DataFrame(columns=[
            "customer_id", "customer_name", "city",
            "total_orders", "total_revenue",
            "avg_order_value", "top_category"
        ])

    # Compute line total
    df["line_total"] = df["quantity"] * df["unit_price"]

    # Aggregate
    summary = df.groupby(
        ["customer_id", "customer_name", "city"], as_index=False
    ).agg(
        total_orders=("order_id", "nunique"),
        total_revenue=("line_total", "sum")
    )

    summary["avg_order_value"] = summary["total_revenue"] / summary["total_orders"]

    # Top category
    cat = df.groupby(["customer_id", "category"])["line_total"].sum().reset_index()
    top_cat = cat.sort_values("line_total", ascending=False).drop_duplicates("customer_id")

    summary = summary.merge(top_cat[["customer_id", "category"]], on="customer_id")

    summary = summary.rename(columns={
        "category": "top_category"
    })

    return summary


def validate(df):
    checks = {
        "no_null_ids": df["customer_id"].notnull().all(),
        "no_null_names": df["customer_name"].notnull().all(),
        "positive_revenue": (df["total_revenue"] > 0).all(),
        "unique_customers": not df["customer_id"].duplicated().any(),
        "positive_orders": (df["total_orders"] > 0).all(),
        "not_empty": not df.empty
    }

    if not all(checks.values()):
        raise ValueError(f"Validation failed: {checks}")

    print("Validation passed")
    return checks


def load(df, engine, csv_path):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    full_path = os.path.join(base_dir, csv_path)

    os.makedirs(os.path.dirname(full_path), exist_ok=True)

    df.to_sql("customer_analytics", engine, if_exists="replace", index=False)
    df.to_csv(full_path, index=False)

    print(f"Saved CSV to: {full_path}")


def main():
    DATABASE_URL = os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg://postgres:postgres@localhost:5432/amman_market"
    )

    engine = create_engine(DATABASE_URL)

    print("Starting ETL pipeline...")

    data = extract(engine)
    summary = transform(data)
    validate(summary)
    load(summary, engine, "output/customer_analytics.csv")

    print("ETL pipeline completed successfully.")
    print(len(data["customers"]))
    print("products:", len(data["products"]))
    print("orders:", len(data["orders"]))
    print("order_items:", len(data["order_items"]))
    print("summary rows:", len(summary))
    print("loaded rows:", len(summary))



if __name__ == "__main__":
    main()