"""
Step 1 gate: verify all data loaders return the expected shapes.
Run from the AnchorDash/ directory:
    python test_services.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from services.data_service import (
    db_exists,
    load_card_share,
    load_card_history,
    load_card_monthly,
    load_monitoring,
    load_monitoring_weekly,
    load_target,
    load_metadata,
    load_mid,
    db_status,
)

def check(name, df):
    if hasattr(df, "shape"):
        print(f"  {name:30s}  rows={df.shape[0]:>5}  cols={df.shape[1]:>3}")
    else:
        print(f"  {name:30s}  {df}")

print("\n=== AnchorDash — Data Service Smoke Test ===\n")
print(f"  DB available: {db_exists()}")
print(f"  Status      : {db_status()}")
print()

loaders = [
    ("card_share",        load_card_share),
    ("card_history",      load_card_history),
    ("card_monthly",      load_card_monthly),
    ("monitoring",        load_monitoring),
    ("monitoring_weekly", load_monitoring_weekly),
    ("target",            load_target),
    ("mid",               load_mid),
]

all_ok = True
for name, fn in loaders:
    try:
        df = fn()
        check(name, df)
    except Exception as e:
        print(f"  {name:30s}  ERROR: {e}")
        all_ok = False

print()
print(f"  metadata: {load_metadata()}")
print()

if all_ok:
    print("All loaders OK — proceed to Step 2.\n")
else:
    print("Some loaders failed — fix before continuing.\n")
    sys.exit(1)
