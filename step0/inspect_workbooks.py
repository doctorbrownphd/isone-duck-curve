"""Print what is actually inside each ISO-NE workbook before we parse anything.

For every workbook under data/raw/isone_zonal/ this reports:
  - sheet names
  - columns on the system sheet, with dtypes
  - the distinct hour labels (to see hour ending 1 to 24, and any "02X")
  - the dates with 23 or 25 rows (daylight saving transitions)
Results also go to step0/output/inspection.txt so we have a record.
"""
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw" / "isone_zonal"
OUT = Path(__file__).resolve().parent / "output"
OUT.mkdir(exist_ok=True)

lines = []


def say(s=""):
    print(s)
    lines.append(s)


for path in sorted(RAW.glob("year=*/*.xls*")):
    xl = pd.ExcelFile(path)
    say("=" * 70)
    say(f"{path.relative_to(ROOT)}")
    say(f"sheets: {xl.sheet_names}")
    sys_name = next((s for s in xl.sheet_names if "CA" in s.upper() and "ISO" in s.upper()), None)
    if sys_name is None:
        say("  !! no system sheet found, check sheet names above")
        continue
    df = xl.parse(sys_name)
    say(f"system sheet: '{sys_name}'  rows={len(df):,}")
    for c in df.columns:
        say(f"  {c!r:28} {str(df[c].dtype):14} e.g. {df[c].iloc[0]!r}")
    hour_col = next((c for c in df.columns if str(c).lower().replace("_", "") in ("hrend", "hourending", "hour")), None)
    date_col = next((c for c in df.columns if str(c).lower() == "date"), None)
    if hour_col is not None:
        labels = sorted(df[hour_col].astype(str).str.strip().unique())
        say(f"hour labels in '{hour_col}': {labels}")
    if date_col is not None:
        per_day = df.groupby(date_col).size()
        odd = per_day[per_day != 24]
        say(f"days with != 24 rows: {len(odd)}")
        for d, n in odd.items():
            say(f"  {d}  rows={n}")
OUT.joinpath("inspection.txt").write_text("\n".join(lines) + "\n")
