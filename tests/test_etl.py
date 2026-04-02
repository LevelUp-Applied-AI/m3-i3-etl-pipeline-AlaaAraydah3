"""Tests for the ETL pipeline.

Write at least 3 tests:
1. test_transform_filters_cancelled — cancelled orders excluded after transform
2. test_transform_filters_suspicious_quantity — quantities > 100 excluded
3. test_validate_catches_nulls — validate() raises ValueError on null customer_id
"""
import pandas as pd
import pytest
from etl_pipeline import transform, validate

def create_dummy_data(cancelled=False, quantity=1, null_id=False):
    customers = pd.DataFrame({
        "customer_id": [None if null_id else 1],
        "name": ["Alice"],
        "city": ["Amman"]
    })
    products = pd.DataFrame({
        "product_id": [101],
        "name": ["Widget"],
        "category": ["Gadgets"],
        "unit_price": [10.0]
    })
    orders = pd.DataFrame({
        "order_id": [1001],
        "customer_id": [1],
        "order_date": ["2026-04-02"],
        "status": ["cancelled" if cancelled else "completed"]
    })
    order_items = pd.DataFrame({
        "item_id": [5001],
        "order_id": [1001],
        "product_id": [101],
        "quantity": [quantity]
    })
    return {"customers": customers, "products": products, "orders": orders, "order_items": order_items}


## --------- TEST --------------

def test_transform_filters_cancelled():
    """Create test DataFrames with a cancelled order. Confirm it's excluded."""
    # TODO: Implement
    data = create_dummy_data(cancelled=True)
    df = transform(data)
    assert df.empty, "Cancelled orders were not filtered out"
    pass


def test_transform_filters_suspicious_quantity():
    """Create test DataFrames with quantity > 100. Confirm it's excluded."""
    # TODO: Implement
    data = create_dummy_data(quantity=101)
    df = transform(data)
    assert df.empty, "Orders with quantity > 100 were not filtered out"
    pass


def test_validate_catches_nulls():
    """Create a DataFrame with null customer_id. Confirm validate() raises ValueError."""
    # TODO: Implement
    data = create_dummy_data(null_id=True)
    df = transform(data)
    with pytest.raises(ValueError):
        validate(df)
    pass


