from __future__ import annotations

from datetime import datetime, timezone
import re
import shutil
from pathlib import Path

import duckdb
import pandas as pd

from include.pipeline_audit import write_pipeline_run


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "include" / "data" / "nammamart"
QUARANTINE_DIR = PROJECT_ROOT / "include" / "quarantine" / "sushanth"
WORK_DIR = PROJECT_ROOT / "include" / ".etl_work" / "sushanth"
WAREHOUSE_PATH = PROJECT_ROOT / "include" / "warehouse_sushanth.duckdb"

SOURCE_FILES = ("orders.csv", "payments.csv", "customers.csv", "products.csv", "stores.csv")
EXPECTED_COLUMNS = {
    "orders.csv": ["order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity", "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status", "channel", "delivery_minutes"],
    "payments.csv": ["payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid", "payment_status", "gateway_ref"],
    "customers.csv": ["customer_id", "full_name", "email", "phone", "city", "area", "pincode", "signup_date", "loyalty_tier", "is_active"],
    "products.csv": ["product_id", "product_name", "category", "sub_category", "brand", "mrp", "cost_price", "unit", "is_perishable", "launch_date"],
    "stores.csv": ["store_id", "store_name", "zone", "area", "pincode", "open_date", "manager_name", "capacity_orders_per_day"],
}
KEY_COLUMNS = {
    "orders.csv": "order_id",
    "payments.csv": "payment_id",
    "customers.csv": "customer_id",
    "products.csv": "product_id",
    "stores.csv": "store_id",
}
REQUIRED_COLUMNS = {
    "orders.csv": ["order_id", "order_ts", "customer_id", "store_id", "product_id", "quantity", "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status"],
    "payments.csv": ["payment_id", "order_id", "payment_ts", "payment_mode", "amount_paid", "payment_status"],
    "customers.csv": ["customer_id"],
    "products.csv": ["product_id", "product_name", "category", "mrp", "cost_price"],
    "stores.csv": ["store_id", "store_name", "zone"],
}
NUMERIC_COLUMNS = {
    "orders.csv": ["quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"],
    "payments.csv": ["amount_paid"],
    "products.csv": ["mrp", "cost_price"],
    "stores.csv": ["capacity_orders_per_day"],
}
DATE_COLUMNS = {
    "orders.csv": ["order_ts"],
    "payments.csv": ["payment_ts"],
    "customers.csv": ["signup_date"],
    "products.csv": ["launch_date"],
    "stores.csv": ["open_date"],
}


def _safe_run_id(run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", run_id).strip("_.") or "manual"


def _check_schema(frame: pd.DataFrame, filename: str) -> None:
    expected = set(EXPECTED_COLUMNS[filename])
    missing = sorted(expected - set(frame.columns))
    extra = sorted(set(frame.columns) - expected)
    if missing or extra:
        raise ValueError(f"Unexpected schema in {filename}; missing={missing}, extra={extra}")


def extract_source(filename: str) -> dict:
    if filename not in SOURCE_FILES:
        raise ValueError(f"Unsupported source file: {filename}")
    path = DATA_DIR / filename
    if not path.is_file():
        raise FileNotFoundError(f"Missing source file: {path}")
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    _check_schema(frame, filename)
    print(f"Extracted {len(frame)} rows from {filename}")
    return {
        "filename": filename,
        "source_path": str(path),
        "rows_extracted": len(frame),
        "extracted_at": datetime.now(timezone.utc).isoformat(),
    }


def _standardize(frame: pd.DataFrame) -> pd.DataFrame:
    clean = frame.copy()
    for column in clean.columns:
        clean[column] = clean[column].astype(str).str.strip()
    if "order_status" in clean:
        clean["order_status"] = clean["order_status"].str.upper()
    if "payment_status" in clean:
        clean["payment_status"] = clean["payment_status"].str.upper()
    if "payment_mode" in clean:
        mode_map = {
            "upi": "UPI", "card": "Card", "cash on delivery": "Cash on Delivery",
            "cod": "Cash on Delivery", "wallet": "Wallet",
        }
        clean["payment_mode"] = clean["payment_mode"].map(
            lambda value: mode_map.get(re.sub(r"\s+", " ", value).casefold(), value)
        )
    for column in ("loyalty_tier", "zone", "channel"):
        if column in clean:
            clean[column] = clean[column].str.title()
    return clean


def _mask_email(value: str) -> str:
    if not value or "@" not in value:
        return "***" if value else ""
    local, domain = value.split("@", 1)
    return f"{local[:2]}***@{domain}"


def _mask_phone(value: str) -> str:
    digits = re.sub(r"\D", "", value)
    if len(digits) == 10:
        return f"{digits[:2]}******{digits[-2:]}"
    return "**********" if value else ""


def _mask_pii(frame: pd.DataFrame) -> pd.DataFrame:
    masked = frame.copy()
    if "email" in masked:
        masked["email"] = masked["email"].map(_mask_email)
    if "phone" in masked:
        masked["phone"] = masked["phone"].map(_mask_phone)
    for column in ("full_name", "manager_name"):
        if column in masked:
            masked[column] = masked[column].map(lambda value: "***" if value else "")
    for column in ("city", "area"):
        if column in masked:
            masked[column] = masked[column].map(lambda value: "***" if value else "")
    if "pincode" in masked:
        masked["pincode"] = masked["pincode"].map(lambda value: "******" if value else "")
    return masked


def _fail(reasons: dict[int, list[str]], row_index: int, message: str) -> None:
    reasons[row_index].append(message)


def _validate_file(filename: str, frame: pd.DataFrame, reasons: dict[int, list[str]], warnings: list) -> None:
    key = KEY_COLUMNS[filename]
    for column in REQUIRED_COLUMNS[filename]:
        for row_index in frame.index[frame[column].eq("")]:
            _fail(reasons, row_index, f"{column} is required")
    duplicates = frame[key].ne("") & frame[key].duplicated(keep="first")
    for row_index in frame.index[duplicates]:
        _fail(reasons, row_index, f"Duplicate {key}; first occurrence retained")

    for column in NUMERIC_COLUMNS.get(filename, []):
        values = pd.to_numeric(frame[column], errors="coerce")
        for row_index in frame.index[frame[column].ne("") & values.isna()]:
            _fail(reasons, row_index, f"{column} is not numeric")

    parsed_dates = {}
    for column in DATE_COLUMNS[filename]:
        values = pd.to_datetime(frame[column], errors="coerce", utc=True)
        parsed_dates[column] = values
        for row_index in frame.index[frame[column].ne("") & values.isna()]:
            _fail(reasons, row_index, f"{column} is not a valid date/time")
    fresh_column = {"orders.csv": "order_ts", "payments.csv": "payment_ts", "customers.csv": "signup_date"}.get(filename)
    if fresh_column:
        future = parsed_dates[fresh_column] > pd.Timestamp.now(tz="UTC")
        for row_index in frame.index[future]:
            _fail(reasons, row_index, f"{fresh_column} is in the future")

    if filename in {"orders.csv", "payments.csv"}:
        allowed_modes = {"UPI", "Card", "Cash on Delivery", "Wallet"}
        for row_index, value in frame["payment_mode"].items():
            if value and value not in allowed_modes:
                _fail(reasons, row_index, f"Invalid payment_mode: {value}")

    if filename == "orders.csv":
        allowed_statuses = {"DELIVERED", "CANCELLED", "RETURNED"}
        for row_index, row in frame.iterrows():
            if row["order_status"] and row["order_status"] not in allowed_statuses:
                _fail(reasons, row_index, f"Invalid order_status: {row['order_status']}")
            quantity, price, discount, amount, delivery = (
                pd.to_numeric(row[column], errors="coerce")
                for column in ("quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes")
            )
            if pd.notna(quantity) and quantity <= 0:
                _fail(reasons, row_index, "quantity must be greater than zero")
            if pd.notna(discount) and not 0 <= discount <= 100:
                _fail(reasons, row_index, "discount_pct must be between 0 and 100")
            if all(pd.notna(value) for value in (quantity, price, discount, amount)):
                expected = round(quantity * price * (1 - discount / 100), 2)
                if abs(expected - amount) > 0.005:
                    _fail(reasons, row_index, "order_amount does not match pricing formula")
            if row["order_status"] == "DELIVERED" and pd.isna(delivery):
                _fail(reasons, row_index, "DELIVERED order requires delivery_minutes")
            if pd.notna(delivery) and (delivery < 0 or delivery >= 120):
                _fail(reasons, row_index, "delivery_minutes must be between 0 and 119")
            if pd.notna(delivery) and ((0 <= delivery < 8) or (35 < delivery < 120)):
                warnings.append((row_index, "delivery time is outside the usual 8-35 minute range"))
    elif filename == "payments.csv":
        for row_index, value in frame["payment_status"].items():
            if value and value not in {"SUCCESS", "FAILED", "REFUNDED"}:
                _fail(reasons, row_index, f"Invalid payment_status: {value}")
    elif filename == "customers.csv":
        for row_index, row in frame.iterrows():
            if row["email"] and not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", row["email"]):
                warnings.append((row_index, "email format is invalid"))
            if row["phone"] and not re.fullmatch(r"\d{10}", row["phone"]):
                warnings.append((row_index, "phone must contain 10 digits"))
            if row["pincode"] and not re.fullmatch(r"\d{6}", row["pincode"]):
                warnings.append((row_index, "pincode must contain 6 digits"))
    elif filename == "products.csv":
        for row_index, row in frame.iterrows():
            mrp = pd.to_numeric(row["mrp"], errors="coerce")
            cost = pd.to_numeric(row["cost_price"], errors="coerce")
            if pd.notna(mrp) and mrp <= 0:
                _fail(reasons, row_index, "mrp must be greater than zero")
            if pd.notna(cost) and pd.notna(mrp) and cost > mrp:
                _fail(reasons, row_index, "cost_price exceeds mrp")
    elif filename == "stores.csv":
        for row_index, row in frame.iterrows():
            if row["zone"] and row["zone"] not in {"North", "South", "East", "West"}:
                _fail(reasons, row_index, f"Invalid zone: {row['zone']}")
            if row["pincode"] and not re.fullmatch(r"\d{6}", row["pincode"]):
                warnings.append((row_index, "pincode must contain 6 digits"))


def _build_dq_results(frames: dict[str, pd.DataFrame], run_id: str) -> pd.DataFrame:
    checked_at = pd.Timestamp.now(tz="UTC").isoformat()
    records = []

    def add_check(
        check_name: str,
        dimension: str,
        severity: str,
        frame: pd.DataFrame,
        failed: pd.Series,
    ) -> None:
        rows_failed = int(failed.fillna(False).sum())
        records.append({
            "run_id": _safe_run_id(run_id),
            "dag_id": "nammamart_etl_sushanth",
            "check_name": check_name,
            "dimension": dimension,
            "severity": severity,
            "rows_checked": len(frame),
            "rows_failed": rows_failed,
            "status": (
                "PASS" if not rows_failed
                else "WARN" if severity == "Warning"
                else "FAIL"
            ),
            "checked_at": checked_at,
        })

    for filename, frame in frames.items():
        required = REQUIRED_COLUMNS[filename]
        missing = frame[required].eq("").any(axis=1)
        add_check(f"{filename.removesuffix('.csv')}_required_fields", "Completeness", "Critical", frame, missing)

        key = KEY_COLUMNS[filename]
        duplicate = frame[key].ne("") & frame[key].duplicated(keep=False)
        add_check(f"{filename.removesuffix('.csv')}_{key}_unique", "Uniqueness", "Critical", frame, duplicate)

        for column in DATE_COLUMNS[filename]:
            parsed = pd.to_datetime(frame[column], errors="coerce", utc=True)
            invalid = frame[column].ne("") & parsed.isna()
            add_check(f"{filename.removesuffix('.csv')}_{column}_valid", "Validity", "Critical", frame, invalid)

    orders = frames["orders.csv"]
    payments = frames["payments.csv"]
    customers = frames["customers.csv"]
    products = frames["products.csv"]
    stores = frames["stores.csv"]
    allowed_modes = {"UPI", "Card", "Cash on Delivery", "Wallet"}
    allowed_statuses = {"DELIVERED", "CANCELLED", "RETURNED"}

    add_check("orders_status_allowed", "Validity", "Critical", orders, ~orders["order_status"].isin(allowed_statuses))
    add_check("orders_payment_mode_allowed", "Validity", "Critical", orders, ~orders["payment_mode"].isin(allowed_modes))
    add_check("payments_mode_allowed", "Validity", "Critical", payments, ~payments["payment_mode"].isin(allowed_modes))
    add_check("payments_status_allowed", "Validity", "Critical", payments, ~payments["payment_status"].isin({"SUCCESS", "FAILED", "REFUNDED"}))
    add_check("customers_email_format", "Validity", "Warning", customers, customers["email"].ne("") & ~customers["email"].str.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+"))
    add_check("customers_phone_10_digits", "Validity", "Warning", customers, customers["phone"].ne("") & ~customers["phone"].str.fullmatch(r"\d{10}"))
    add_check("customers_pincode_6_digits", "Validity", "Warning", customers, customers["pincode"].ne("") & ~customers["pincode"].str.fullmatch(r"\d{6}"))
    add_check("stores_zone_allowed", "Validity", "Critical", stores, ~stores["zone"].isin({"North", "South", "East", "West"}))

    quantity = pd.to_numeric(orders["quantity"], errors="coerce")
    price = pd.to_numeric(orders["unit_price"], errors="coerce")
    discount = pd.to_numeric(orders["discount_pct"], errors="coerce")
    amount = pd.to_numeric(orders["order_amount"], errors="coerce")
    delivery = pd.to_numeric(orders["delivery_minutes"], errors="coerce")
    add_check("orders_quantity_positive", "Accuracy", "Critical", orders, quantity.notna() & quantity.le(0))
    add_check("orders_discount_range", "Accuracy", "Critical", orders, discount.notna() & ~discount.between(0, 100))
    expected_amount = (quantity * price * (1 - discount / 100)).round(2)
    amount_mismatch = expected_amount.notna() & amount.notna() & (expected_amount - amount).abs().gt(0.005)
    add_check("orders_amount_formula", "Accuracy", "Critical", orders, amount_mismatch)
    invalid_delivery = delivery.notna() & ((delivery < 0) | (delivery >= 120))
    missing_delivery = orders["order_status"].eq("DELIVERED") & delivery.isna()
    add_check("orders_delivery_bounds", "Accuracy", "Critical", orders, invalid_delivery | missing_delivery)
    unusual_delivery = delivery.notna() & (((delivery >= 0) & (delivery < 8)) | ((delivery > 35) & (delivery < 120)))
    add_check("orders_delivery_typical_range", "Accuracy", "Warning", orders, unusual_delivery)

    mrp = pd.to_numeric(products["mrp"], errors="coerce")
    cost = pd.to_numeric(products["cost_price"], errors="coerce")
    add_check("products_mrp_positive", "Accuracy", "Critical", products, mrp.notna() & mrp.le(0))
    add_check("products_cost_not_above_mrp", "Accuracy", "Critical", products, cost.notna() & mrp.notna() & cost.gt(mrp))

    for column, target in (("customer_id", customers), ("product_id", products), ("store_id", stores)):
        target_ids = set(target[KEY_COLUMNS[{"customer_id": "customers.csv", "product_id": "products.csv", "store_id": "stores.csv"}[column]]])
        failed = orders[column].ne("") & ~orders[column].isin(target_ids)
        add_check(f"orders_{column}_foreign_key", "Consistency", "Critical", orders, failed)
    order_ids = set(orders["order_id"])
    payment_order_missing = payments["order_id"].ne("") & ~payments["order_id"].isin(order_ids)
    add_check("payments_order_foreign_key", "Consistency", "Critical", payments, payment_order_missing)
    order_amounts = orders.drop_duplicates("order_id").set_index("order_id")["order_amount"].to_dict()
    payment_expected = payments["order_id"].map(order_amounts)
    paid = pd.to_numeric(payments["amount_paid"], errors="coerce")
    payment_mismatch = payments["payment_status"].eq("SUCCESS") & payment_expected.notna() & paid.notna() & (paid - pd.to_numeric(payment_expected, errors="coerce")).abs().gt(0.005)
    add_check("successful_payment_amount_matches_order", "Consistency", "Critical", payments, payment_mismatch)

    for filename, column in (("orders.csv", "order_ts"), ("payments.csv", "payment_ts"), ("customers.csv", "signup_date")):
        frame = frames[filename]
        parsed = pd.to_datetime(frame[column], errors="coerce", utc=True)
        future = parsed > pd.Timestamp.now(tz="UTC")
        add_check(f"{filename.removesuffix('.csv')}_{column}_not_future", "Timeliness", "Critical", frame, future)

    latest_order_ts = pd.to_datetime(orders["order_ts"], errors="coerce", utc=True).max()
    stale_orders = pd.notna(latest_order_ts) and (
        latest_order_ts < pd.Timestamp.now(tz="UTC") - pd.Timedelta(days=1)
    )
    records.append({
        "run_id": _safe_run_id(run_id),
        "dag_id": "nammamart_etl_sushanth",
        "check_name": "orders_latest_timestamp_within_1_day",
        "dimension": "Timeliness",
        "severity": "Warning",
        "rows_checked": 1,
        "rows_failed": int(stale_orders),
        "status": "WARN" if stale_orders else "PASS",
        "checked_at": checked_at,
    })

    volume_failed = int(len(orders) < 2412 * 0.1)
    records.append({
        "run_id": _safe_run_id(run_id),
        "dag_id": "nammamart_etl_sushanth",
        "check_name": "orders_volume_drop_90pct",
        "dimension": "Volume",
        "severity": "Warning",
        "rows_checked": 1,
        "rows_failed": volume_failed,
        "status": "WARN" if volume_failed else "PASS",
        "checked_at": checked_at,
    })

    return pd.DataFrame(records, columns=[
        "run_id", "dag_id", "check_name", "dimension", "severity",
        "rows_checked", "rows_failed", "status", "checked_at",
    ])


def validate_sources(extracted_files: list[dict], run_id: str) -> dict:
    safe_run_id = _safe_run_id(run_id)
    run_work_dir = WORK_DIR / safe_run_id
    run_work_dir.mkdir(parents=True, exist_ok=True)
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    raw_frames, clean_frames, failures, warnings_by_file = {}, {}, {}, {}

    for item in extracted_files:
        filename = item["filename"]
        raw = pd.read_csv(item["source_path"], dtype=str, keep_default_na=False)
        _check_schema(raw, filename)
        clean = _standardize(raw)
        reasons = {index: [] for index in clean.index}
        warnings = []
        _validate_file(filename, clean, reasons, warnings)
        raw_frames[filename], clean_frames[filename] = raw, clean
        failures[filename], warnings_by_file[filename] = reasons, warnings

    good_ids = {}
    for filename, key in (("customers.csv", "customer_id"), ("products.csv", "product_id"), ("stores.csv", "store_id")):
        good_ids[filename] = {
            clean_frames[filename].at[index, key]
            for index in clean_frames[filename].index
            if not failures[filename][index] and clean_frames[filename].at[index, key]
        }

    orders = clean_frames["orders.csv"]
    for index, row in orders.iterrows():
        for column, filename, key in (
            ("customer_id", "customers.csv", "customer_id"),
            ("product_id", "products.csv", "product_id"),
            ("store_id", "stores.csv", "store_id"),
        ):
            if row[column] and row[column] not in good_ids[filename]:
                _fail(failures["orders.csv"], index, f"{column} not found in clean {filename}")

    clean_order_indices = [index for index in orders.index if not failures["orders.csv"][index]]
    order_lookup = {
        orders.at[index, "order_id"]: orders.at[index, "order_amount"]
        for index in clean_order_indices
    }
    payments = clean_frames["payments.csv"]
    for index, row in payments.iterrows():
        if row["order_id"] and row["order_id"] not in order_lookup:
            _fail(failures["payments.csv"], index, "order_id not found in clean orders")
        if row["payment_status"] == "SUCCESS" and row["order_id"] in order_lookup:
            paid = pd.to_numeric(row["amount_paid"], errors="coerce")
            expected = pd.to_numeric(order_lookup[row["order_id"]], errors="coerce")
            if pd.notna(paid) and pd.notna(expected) and abs(paid - expected) > 0.005:
                _fail(failures["payments.csv"], index, "successful payment does not match order_amount")

    dq_frames = {filename: _standardize(frame) for filename, frame in raw_frames.items()}
    dq_results = _build_dq_results(dq_frames, safe_run_id)
    manifest = {
        "run_id": safe_run_id,
        "work_dir": str(run_work_dir),
        "files": {},
        "dq_results": dq_results.to_dict(orient="records"),
    }
    for filename in SOURCE_FILES:
        raw, clean, reasons = raw_frames[filename], clean_frames[filename], failures[filename]
        failed_indices = [index for index, messages in reasons.items() if messages]
        passed_indices = [index for index, messages in reasons.items() if not messages]
        failed_rows = raw.loc[failed_indices].copy()
        failed_rows["failure_reason"] = ["; ".join(reasons[index]) for index in failed_indices]
        failed_rows = _mask_pii(failed_rows)
        quarantine_path = QUARANTINE_DIR / f"{Path(filename).stem}_failed_{safe_run_id}.csv"
        failed_rows.to_csv(quarantine_path, index=False)

        passed_rows = clean.loc[passed_indices].copy()
        if filename == "customers.csv":
            passed_rows = _mask_pii(passed_rows)
        for column in NUMERIC_COLUMNS.get(filename, []):
            passed_rows[column] = pd.to_numeric(passed_rows[column], errors="coerce")
        for column in DATE_COLUMNS[filename]:
            passed_rows[column] = pd.to_datetime(passed_rows[column], errors="coerce", utc=True)
        passed_path = run_work_dir / filename
        passed_rows.to_csv(passed_path, index=False)

        counts = {
            "rows_extracted": len(raw),
            "rows_passed": len(passed_rows),
            "rows_quarantined": len(failed_rows),
            "warnings": len(warnings_by_file[filename]),
        }
        print(f"{filename}: {counts}")
        for row_index, message in warnings_by_file[filename]:
            print(f"Warning in {filename}, row {row_index + 2}: {message}")
        manifest["files"][filename] = {
            "passed_path": str(passed_path),
            "quarantine_path": str(quarantine_path),
            **counts,
        }
    return manifest


def _read_passed(manifest: dict, filename: str) -> pd.DataFrame:
    return pd.read_csv(manifest["files"][filename]["passed_path"])


def _build_reconciliation(clean_orders: pd.DataFrame) -> pd.DataFrame:
    columns = ["order_id", "payment_id", "order_amount", "amount_paid", "order_payment_mode", "payment_mode", "reason"]
    payments = _standardize(pd.read_csv(DATA_DIR / "payments.csv", dtype=str, keep_default_na=False))
    orders = clean_orders.set_index("order_id").to_dict(orient="index")
    successes, mismatches = {}, []
    for _, payment in payments.iterrows():
        if payment["payment_status"] != "SUCCESS":
            continue
        order_id = payment["order_id"]
        successes.setdefault(order_id, []).append(payment)
        order = orders.get(order_id)
        paid = pd.to_numeric(payment["amount_paid"], errors="coerce")
        if order is None:
            mismatches.append({"order_id": order_id, "payment_id": payment["payment_id"], "amount_paid": paid, "payment_mode": payment["payment_mode"], "reason": "Payment has no order in the clean ETL fact"})
            continue
        reasons = []
        if order["order_status"] != "DELIVERED":
            reasons.append("Successful payment is attached to a non-delivered order")
        if pd.isna(paid) or abs(float(paid) - float(order["order_amount"])) > 0.005:
            reasons.append("Payment amount does not match order_amount")
        if payment["payment_mode"] != order["payment_mode"]:
            reasons.append("Payment mode does not match order")
        if reasons:
            mismatches.append({"order_id": order_id, "payment_id": payment["payment_id"], "order_amount": order["order_amount"], "amount_paid": paid, "order_payment_mode": order["payment_mode"], "payment_mode": payment["payment_mode"], "reason": "; ".join(reasons)})
    for order_id, order in orders.items():
        if order["order_status"] == "DELIVERED" and not successes.get(order_id):
            mismatches.append({"order_id": order_id, "order_amount": order["order_amount"], "order_payment_mode": order["payment_mode"], "reason": "Delivered order has no successful payment"})
    return pd.DataFrame(mismatches, columns=columns)


def transform_data(validated: dict, run_id: str) -> dict:
    work_dir = Path(validated["work_dir"])
    orders = _read_passed(validated, "orders.csv")
    customers = _read_passed(validated, "customers.csv").drop_duplicates("customer_id")
    products = _read_passed(validated, "products.csv").drop_duplicates("product_id")
    stores = _mask_pii(
        _read_passed(validated, "stores.csv").drop_duplicates("store_id")
    )

    for column in NUMERIC_COLUMNS["orders.csv"]:
        orders[column] = pd.to_numeric(orders[column], errors="coerce")
    orders["order_ts"] = pd.to_datetime(orders["order_ts"], errors="coerce", utc=True)
    orders["order_date"] = orders["order_ts"].dt.date
    for column in ("mrp", "cost_price"):
        products[column] = pd.to_numeric(products[column], errors="coerce")
    stores["capacity_orders_per_day"] = pd.to_numeric(stores["capacity_orders_per_day"], errors="coerce")

    enriched = orders.merge(customers[["customer_id"]], on="customer_id", how="inner", validate="many_to_one")
    enriched = enriched.merge(products[["product_id", "category", "cost_price"]], on="product_id", how="inner", validate="many_to_one")
    enriched = enriched.merge(stores[["store_id", "zone"]], on="store_id", how="inner", validate="many_to_one")
    enriched["net_revenue"] = enriched["order_amount"].where(enriched["order_status"].eq("DELIVERED"), 0.0)
    enriched["gross_margin"] = (
        enriched["net_revenue"] - enriched["quantity"] * enriched["cost_price"]
    ).where(enriched["order_status"].eq("DELIVERED"), 0.0)
    enriched["within_15_min"] = enriched["order_status"].eq("DELIVERED") & enriched["delivery_minutes"].le(15)
    fact = enriched[["order_id", "order_ts", "order_date", "customer_id", "product_id", "store_id", "quantity", "unit_price", "discount_pct", "order_amount", "payment_mode", "order_status", "channel", "delivery_minutes", "net_revenue", "gross_margin", "within_15_min"]]
    reconciliation = _build_reconciliation(orders)
    dq_columns = [
        "run_id", "dag_id", "check_name", "dimension", "severity",
        "rows_checked", "rows_failed", "status", "checked_at",
    ]

    outputs = {
        "etl_fact_orders": fact,
        "etl_dim_customer": customers,
        "etl_dim_product": products,
        "etl_dim_store": stores,
        "etl_payment_reconciliation": reconciliation,
        "dq_results": pd.DataFrame(validated["dq_results"], columns=dq_columns),
    }
    paths = {}
    for table_name, frame in outputs.items():
        path = work_dir / f"{table_name}.csv"
        frame.to_csv(path, index=False)
        paths[table_name] = str(path)
    print(f"Transformed {len(fact)} clean orders for run {_safe_run_id(run_id)}")
    return {"run_id": _safe_run_id(run_id), "work_dir": str(work_dir), "outputs": paths}


def load_to_duckdb(transformed: dict) -> dict:
    WAREHOUSE_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(WAREHOUSE_PATH))
    table_relations = {
        "etl_fact_orders": "fact_df",
        "etl_dim_customer": "customer_df",
        "etl_dim_product": "product_df",
        "etl_dim_store": "store_df",
        "etl_payment_reconciliation": "reconciliation_df",
    }
    row_counts = {}
    try:
        for table_name, relation in table_relations.items():
            frame = pd.read_csv(transformed["outputs"][table_name])
            connection.register(relation, frame)
            connection.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM {relation}")
            row_counts[table_name] = len(frame)
            connection.unregister(relation)
        dq_frame = pd.read_csv(transformed["outputs"]["dq_results"])
        connection.execute("""
            CREATE TABLE IF NOT EXISTS dq_results (
                run_id VARCHAR,
                dag_id VARCHAR,
                check_name VARCHAR,
                dimension VARCHAR,
                severity VARCHAR,
                rows_checked BIGINT,
                rows_failed BIGINT,
                status VARCHAR,
                checked_at TIMESTAMPTZ
            )
        """)
        connection.execute(
            "DELETE FROM dq_results WHERE run_id = ? AND dag_id = ?",
            [transformed["run_id"], "nammamart_etl_sushanth"],
        )
        connection.register("dq_frame", dq_frame)
        connection.execute("""
            INSERT INTO dq_results (
                run_id, dag_id, check_name, dimension, severity,
                rows_checked, rows_failed, status, checked_at
            )
            SELECT
                run_id, dag_id, check_name, dimension, severity,
                rows_checked, rows_failed, status, checked_at
            FROM dq_frame
        """)
        connection.unregister("dq_frame")
        row_counts["dq_results"] = len(dq_frame)
    finally:
        connection.close()
    return {"run_id": transformed["run_id"], "work_dir": transformed["work_dir"], "warehouse_path": str(WAREHOUSE_PATH), "row_counts": row_counts}


def print_reports(load_result: dict) -> None:
    connection = duckdb.connect(load_result["warehouse_path"])
    try:
        connection.execute("""
            CREATE OR REPLACE TABLE etl_daily_zone_revenue AS
            SELECT f.order_date, s.zone, SUM(f.net_revenue) AS net_revenue
            FROM etl_fact_orders f JOIN etl_dim_store s USING (store_id)
            WHERE f.order_status = 'DELIVERED'
            GROUP BY f.order_date, s.zone ORDER BY f.order_date, s.zone
        """)
        connection.execute("""
            CREATE OR REPLACE TABLE etl_category_margin AS
            SELECT p.category, SUM(f.net_revenue) AS net_revenue,
                   SUM(f.gross_margin) AS gross_margin
            FROM etl_fact_orders f JOIN etl_dim_product p USING (product_id)
            WHERE f.order_status = 'DELIVERED'
            GROUP BY p.category ORDER BY p.category
        """)
        print("ETL daily zone revenue:")
        print(connection.execute("SELECT * FROM etl_daily_zone_revenue").df().to_string(index=False))
        print("ETL category margin:")
        print(connection.execute("SELECT * FROM etl_category_margin").df().to_string(index=False))
        print("ETL data-quality results:")
        print(connection.execute(
            "SELECT check_name, dimension, severity, rows_checked, rows_failed, status "
            "FROM dq_results WHERE dag_id = 'nammamart_etl_sushanth' "
            "ORDER BY dimension, check_name"
        ).df().to_string(index=False))
        print(f"ETL row counts: {load_result['row_counts']}")
    finally:
        connection.close()
        shutil.rmtree(load_result["work_dir"], ignore_errors=True)


def record_pipeline_run(context: dict) -> dict:
    task_instance = context["ti"]
    dag_run = context["dag_run"]
    extract_ids = [f"extract_{Path(filename).stem}" for filename in SOURCE_FILES]
    extracted = [task_instance.xcom_pull(task_ids=task_id) for task_id in extract_ids]
    rows_extracted = sum(
        int(result.get("rows_extracted", 0))
        for result in extracted
        if isinstance(result, dict)
    )
    validation = task_instance.xcom_pull(task_ids="validate") or {}
    rows_quarantined = sum(
        int(result.get("rows_quarantined", 0))
        for result in validation.get("files", {}).values()
    )
    load_result = task_instance.xcom_pull(task_ids="load") or {}
    rows_loaded = int(load_result.get("row_counts", {}).get("etl_fact_orders", 0))
    required_outputs = extract_ids + ["validate", "transform", "load", "report"]
    failed = any(
        task_instance.xcom_pull(task_ids=task_id) is None
        for task_id in required_outputs
    )
    extract_times = [
        datetime.fromisoformat(result["extracted_at"])
        for result in extracted
        if isinstance(result, dict) and result.get("extracted_at")
    ]
    start_time = min(extract_times) if extract_times else task_instance.start_date
    return write_pipeline_run(
        str(WAREHOUSE_PATH),
        context["run_id"],
        "nammamart_etl_sushanth",
        start_time,
        rows_extracted,
        rows_loaded,
        rows_quarantined,
        "FAILED" if failed else "SUCCESS",
    )