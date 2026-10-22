
from pathlib import Path
import pandas as pd

WORKSPACE_ROOT = Path(__file__).resolve().parent.parent
DATA = WORKSPACE_ROOT / "include" / "data" / "nammamart"
OUT = WORKSPACE_ROOT / "result" / "profiling"
OUT.mkdir(parents=True, exist_ok=True)

FILES = [
    "orders.csv",
    "payments.csv",
    "customers.csv",
    "products.csv",
    "stores.csv",
    "orders_2026-10-01_increment.csv",
]

KEYS = {
    "orders.csv": "order_id",
    "payments.csv": "payment_id",
    "customers.csv": "customer_id",
    "products.csv": "product_id",
    "stores.csv": "store_id",
    "orders_2026-10-01_increment.csv": "order_id",
}

NUMERIC = {
    "orders.csv": [
        "quantity", "unit_price", "discount_pct",
        "order_amount", "delivery_minutes",
    ],
    "payments.csv": ["amount_paid"],
    "products.csv": ["mrp", "cost_price"],
    "stores.csv": ["capacity_orders_per_day"],
    "orders_2026-10-01_increment.csv": [
        "quantity", "unit_price", "discount_pct",
        "order_amount", "delivery_minutes",
    ],
}

DATES = {
    "orders.csv": ["order_ts"],
    "payments.csv": ["payment_ts"],
    "customers.csv": ["signup_date"],
    "products.csv": ["launch_date"],
    "stores.csv": ["open_date"],
    "orders_2026-10-01_increment.csv": ["order_ts"],
}

CATEGORIES = {
    "orders.csv": ["order_status", "payment_mode", "channel"],
    "payments.csv": ["payment_mode", "payment_status"],
    "customers.csv": ["loyalty_tier", "is_active"],
    "products.csv": ["category", "sub_category", "unit", "is_perishable"],
    "stores.csv": ["zone"],
    "orders_2026-10-01_increment.csv": [
        "order_status", "payment_mode", "channel",
    ],
}

REQUIRED_COLUMNS = {
    "orders.csv": [
        "order_id", "order_ts", "customer_id", "store_id", "product_id",
        "quantity", "unit_price", "discount_pct", "order_amount",
        "payment_mode", "order_status",
    ],
    "payments.csv": [
        "payment_id", "order_id", "payment_ts", "payment_mode",
        "amount_paid", "payment_status",
    ],
    "customers.csv": ["customer_id"],
    "products.csv": ["product_id", "product_name", "category", "mrp", "cost_price"],
    "stores.csv": ["store_id", "store_name", "zone"],
    "orders_2026-10-01_increment.csv": [
        "order_id", "order_ts", "customer_id", "store_id", "product_id",
        "quantity", "unit_price", "discount_pct", "order_amount",
        "payment_mode", "order_status",
    ],
}

issues = []
profiles = []
column_profiles = []
frames = {}


def issue(file, key, column, problem, dimension,
          rule, severity="Critical"):
    issues.append({
        "file": file,
        "row_key": key,
        "column": column,
        "problem": problem,
        "quality_dimension": dimension,
        "proposed_rule": rule,
        "severity": severity,
    })


# 1. Load all files as strings so profiling doesn't silently
#    coerce source values into different representations.
for filename in FILES:
    path = DATA / filename
    if not path.exists():
        raise FileNotFoundError(f"Missing source file: {path}")

    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    frames[filename] = df

    print(f"\n{'=' * 65}\n{filename}: {len(df)} rows")
    print("Columns:", list(df.columns))

    key = KEYS[filename]

    # Required key and duplicate-key checks.
    if key not in df.columns:
        raise ValueError(f"{filename} is missing key column {key}")

    normalized_keys = df[key].astype(str).str.strip()
    duplicate_keys = normalized_keys.duplicated(keep=False) & normalized_keys.ne("")
    for idx, row in df.iterrows():
        row_key = row[key].strip() or f"row_{idx + 2}"
        if not row[key].strip():
            issue(filename, row_key, key, "Missing primary key",
                  "Completeness", f"{key} must not be blank")
        if duplicate_keys.iloc[idx]:
            issue(filename, row_key, key, "Duplicate primary key",
                  "Uniqueness", f"{key} must be unique")

    # Null/blank counts.
    for col in df.columns:
        blanks = df[col].astype(str).str.strip().eq("")
        count = int(blanks.sum())
        if count:
            print(f"Blank {col}: {count}")
            for idx in df.index[blanks]:
                row_key = df.at[idx, key].strip() or f"row_{idx + 2}"
                severity = "Critical" if col in REQUIRED_COLUMNS[filename] else "Warning"
                issue(filename, row_key, col, "Blank value",
                      "Completeness", f"{col} must be populated", severity)

    # Persist a profile for every source column, including extrema and category values.
    for col in df.columns:
        profile = {
            "file": filename,
            "column": col,
            "blank_count": int(df[col].astype(str).str.strip().eq("").sum()),
            "distinct_count": int(df[col].nunique(dropna=False)),
            "min_value": "",
            "max_value": "",
            "distinct_values": "",
        }
        if col in NUMERIC.get(filename, []):
            values = pd.to_numeric(df[col], errors="coerce").dropna()
            if not values.empty:
                profile["min_value"] = str(values.min())
                profile["max_value"] = str(values.max())
        elif col in DATES.get(filename, []):
            values = pd.to_datetime(df[col], errors="coerce", utc=True).dropna()
            if not values.empty:
                profile["min_value"] = values.min().isoformat()
                profile["max_value"] = values.max().isoformat()
        if col in CATEGORIES.get(filename, []):
            profile["distinct_values"] = " | ".join(
                sorted(df[col].astype(str).unique().tolist())
            )
        column_profiles.append(profile)

    # Numeric columns: parseability, min/max, negative values.
    for col in NUMERIC.get(filename, []):
        if col not in df.columns:
            continue
        values = pd.to_numeric(df[col], errors="coerce")
        print(f"{col}: min={values.min()}, max={values.max()}")
        invalid = values.isna() & df[col].str.strip().ne("")
        for idx in df.index[invalid]:
            issue(filename, df.at[idx, key], col,
                  "Not a valid number", "Validity",
                  f"{col} must be numeric")

    # Date columns: parseability and range.
    for col in DATES.get(filename, []):
        if col not in df.columns:
            continue
        values = pd.to_datetime(df[col], errors="coerce", utc=True)
        valid_raw = df[col].str.strip().ne("")
        invalid = values.isna() & valid_raw
        print(f"{col}: min={values.min()}, max={values.max()}")
        for idx in df.index[invalid]:
            issue(filename, df.at[idx, key], col,
                  "Invalid date or timestamp", "Validity",
                  f"{col} must parse as a date/time")

    # Distinct category values.
    for col in CATEGORIES.get(filename, []):
        if col in df.columns:
            print(f"{col} distinct:", sorted(df[col].unique().tolist()))

    profiles.append({
        "file": filename,
        "rows": len(df),
        "columns": len(df.columns),
        "duplicate_key_rows": int(duplicate_keys.sum()),
        "blank_cells": int(
            df.apply(lambda s: s.astype(str).str.strip().eq("")).sum().sum()
        ),
    })


# 2. Referential integrity checks for the base September files.
orders = frames["orders.csv"]
payments = frames["payments.csv"]
customers = frames["customers.csv"]
products = frames["products.csv"]
stores = frames["stores.csv"]

def check_fk(source, source_key, target, target_key, filename):
    if source_key not in source.columns or target_key not in target.columns:
        return

    target_values = set(target[target_key].str.strip()) - {""}

    # Check only populated foreign keys.
    populated = source[source_key].str.strip().ne("")
    bad = populated & ~source[source_key].str.strip().isin(target_values)

    for idx in source.index[bad]:
        issue(
            filename,
            source.at[idx, KEYS[filename]],
            source_key,
            f"Unknown reference: {source.at[idx, source_key]}",
            "Consistency",
            f"{source_key} must exist in {target_key}",
        )


for filename in ["orders.csv", "orders_2026-10-01_increment.csv"]:
    source = frames[filename]
    check_fk(source, "customer_id", customers, "customer_id", filename)
    check_fk(source, "product_id", products, "product_id", filename)
    check_fk(source, "store_id", stores, "store_id", filename)

order_ids = set(orders["order_id"].str.strip())
bad_payment = ~payments["order_id"].str.strip().isin(order_ids)
for idx in payments.index[bad_payment]:
    issue("payments.csv", payments.at[idx, "payment_id"], "order_id",
          "Payment references unknown order", "Consistency",
          "Payment order_id must exist in orders.csv")


# 3. Business rules for orders and incremental orders.
for filename in ["orders.csv", "orders_2026-10-01_increment.csv"]:
    df = frames[filename]
    parsed_order_ts = pd.to_datetime(df["order_ts"], errors="coerce", utc=True)
    future_order_ts = parsed_order_ts > pd.Timestamp.now(tz="UTC")
    for idx in df.index[future_order_ts]:
        issue(filename, df.at[idx, "order_id"], "order_ts",
              "Order timestamp is in the future", "Timeliness",
              "order_ts must not be in the future")

    for idx, row in df.iterrows():
        key = row["order_id"]
        quantity = pd.to_numeric(row["quantity"], errors="coerce")
        price = pd.to_numeric(row["unit_price"], errors="coerce")
        discount = pd.to_numeric(row["discount_pct"], errors="coerce")
        amount = pd.to_numeric(row["order_amount"], errors="coerce")
        delivery = pd.to_numeric(row["delivery_minutes"], errors="coerce")
        status = row["order_status"].strip().upper()
        mode = row["payment_mode"].strip().upper()

        if pd.notna(quantity) and quantity <= 0:
            issue(filename, key, "quantity", "Quantity must be positive",
                  "Accuracy", "quantity > 0")

        if pd.notna(discount) and not 0 <= discount <= 100:
            issue(filename, key, "discount_pct", "Discount outside 0-100",
                  "Validity", "0 <= discount_pct <= 100")

        if all(pd.notna(x) for x in [quantity, price, discount, amount]):
            expected = round(quantity * price * (1 - discount / 100), 2)
            if abs(expected - amount) > 0.005:
                issue(filename, key, "order_amount",
                      f"Expected {expected:.2f}, got {amount:.2f}",
                      "Accuracy", "Amount must match pricing formula")

        if status not in {"DELIVERED", "CANCELLED", "RETURNED"}:
            issue(filename, key, "order_status", f"Invalid status: {status}",
                  "Validity", "Status must be an allowed value")

        if mode not in {"UPI", "CARD", "CASH ON DELIVERY", "WALLET"}:
            issue(filename, key, "payment_mode", f"Invalid mode: {mode}",
                  "Validity", "Payment mode must be an allowed value")

        if status == "DELIVERED" and pd.isna(delivery):
            issue(filename, key, "delivery_minutes",
                  "Delivered order has no delivery time",
                  "Completeness", "Delivered orders need delivery_minutes")

        if pd.notna(delivery) and (delivery < 0 or delivery >= 120):
            issue(filename, key, "delivery_minutes",
                  f"Implausible delivery time: {delivery}",
                  "Accuracy", "Delivery time must be between 0 and 120")

# 4. Product-specific rules.
for idx, row in products.iterrows():
    mrp = pd.to_numeric(row["mrp"], errors="coerce")
    cost = pd.to_numeric(row["cost_price"], errors="coerce")
    key = row["product_id"]

    if pd.notna(mrp) and mrp <= 0:
        issue("products.csv", key, "mrp", "MRP must be positive",
              "Accuracy", "mrp > 0")
    if pd.notna(cost) and pd.notna(mrp) and cost > mrp:
        issue("products.csv", key, "cost_price", "Cost exceeds MRP",
              "Accuracy", "cost_price <= mrp")

# Customer and master-data format checks.
for idx, row in customers.iterrows():
    key = row["customer_id"].strip() or f"row_{idx + 2}"
    email = row["email"].strip()
    phone = row["phone"].strip()
    if email and not pd.Series([email]).str.fullmatch(
            r"[^\s@]+@[^\s@]+\.[^\s@]+"
    ).iloc[0]:
        issue("customers.csv", key, "email", "Invalid email format",
              "Validity", "email must have a valid email format", "Warning")
    if phone and not pd.Series([phone]).str.fullmatch(r"\d{10}").iloc[0]:
        issue("customers.csv", key, "phone", "Phone must contain 10 digits",
              "Validity", "phone must be exactly 10 digits", "Warning")
    pincode = row["pincode"].strip()
    if pincode and not pd.Series([pincode]).str.fullmatch(r"\d{6}").iloc[0]:
        issue("customers.csv", key, "pincode", "Pincode must contain 6 digits",
              "Validity", "pincode must be exactly 6 digits", "Warning")

for idx, row in stores.iterrows():
    pincode = row["pincode"].strip()
    if pincode and not pd.Series([pincode]).str.fullmatch(r"\d{6}").iloc[0]:
        issue("stores.csv", row["store_id"], "pincode",
              "Pincode must contain 6 digits", "Validity",
              "pincode must be exactly 6 digits", "Warning")

signup_dates = pd.to_datetime(customers["signup_date"], errors="coerce", utc=True)
for idx in customers.index[signup_dates > pd.Timestamp.now(tz="UTC")]:
    issue("customers.csv", customers.at[idx, "customer_id"], "signup_date",
          "Signup date is in the future", "Timeliness",
          "signup_date must not be in the future")

payment_dates = pd.to_datetime(payments["payment_ts"], errors="coerce", utc=True)
for idx in payments.index[payment_dates > pd.Timestamp.now(tz="UTC")]:
    issue("payments.csv", payments.at[idx, "payment_id"], "payment_ts",
          "Payment timestamp is in the future", "Timeliness",
          "payment_ts must not be in the future")

for filename in ["orders.csv", "orders_2026-10-01_increment.csv"]:
    df = frames[filename]
    delivery_values = pd.to_numeric(df["delivery_minutes"], errors="coerce")
    unusual_delivery = delivery_values.notna() & (
        ((delivery_values < 8) & (delivery_values >= 0)) |
        ((delivery_values > 35) & (delivery_values < 120))
    )
    for idx in df.index[unusual_delivery]:
        issue(filename, df.at[idx, "order_id"], "delivery_minutes",
              f"Delivery time outside usual 8-35 minute range: {delivery_values.at[idx]}",
              "Accuracy", "Typical delivery time is 8-35 minutes", "Warning")

# Payment amount check for successful payments.
order_amounts = orders.drop_duplicates("order_id").set_index("order_id")
for idx, row in payments.iterrows():
    if row["payment_status"].strip().upper() != "SUCCESS":
        continue
    oid = row["order_id"]
    paid = pd.to_numeric(row["amount_paid"], errors="coerce")
    if oid in order_amounts.index and pd.notna(paid):
        expected = pd.to_numeric(
            order_amounts.loc[oid, "order_amount"], errors="coerce"
        )
        if pd.notna(expected) and abs(paid - expected) > 0.01:
            issue("payments.csv", row["payment_id"], "amount_paid",
                  f"Paid {paid:.2f}, order amount {expected:.2f}",
                  "Consistency", "Successful payment must match order amount")

# Persist profiling results.
pd.DataFrame(profiles).to_csv(OUT / "file_profile.csv", index=False)
pd.DataFrame(column_profiles).to_csv(OUT / "column_profile.csv", index=False)
pd.DataFrame(issues, columns=[
    "file", "row_key", "column", "problem",
    "quality_dimension", "proposed_rule", "severity"
]).to_csv(OUT / "data_issues_log.csv", index=False)
issue_frame = pd.DataFrame(issues)
if issue_frame.empty:
    issue_summary = pd.DataFrame(
        columns=["file", "quality_dimension", "severity", "issue_count"]
    )
else:
    issue_summary = (
        issue_frame.groupby(["file", "quality_dimension", "severity"])
        .size().rename("issue_count").reset_index()
    )
issue_summary.to_csv(OUT / "issue_summary.csv", index=False)

print("\n=== PROFILE SUMMARY ===")
print(pd.DataFrame(profiles).to_string(index=False))
print(f"\nIssues logged: {len(issues)}")
print("Issues by file:")
print(issue_frame.groupby("file").size().to_string() if not issue_frame.empty else "None")
print(f"Outputs saved under: {OUT.resolve()}")
