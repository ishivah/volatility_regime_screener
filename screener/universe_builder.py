import os
import io
import sys
import time
import argparse
from typing import List

import pandas as pd
import requests
import yfinance as yf

# Endpoints of NSE 
NIFTY100_CSV = "https://nsearchives.nseindia.com/content/indices/ind_nifty100list.csv"   # from NSE page
MIDCAP150_CSV = "https://www.niftyindices.com/IndexConstituent/ind_niftymidcap150list.csv"
SMALLCAP250_CSV = "https://www.niftyindices.com/IndexConstituent/ind_niftysmallcap250list.csv"
NIFTY500_CSV = "https://www.niftyindices.com/IndexConstituent/ind_nifty500list.csv"

OUT_DIR = "config"

TARGETS = {
  "largecap" : ("largecap.csv", NIFTY100_CSV),
  "midcap" : ("midcap.csv", MIDCAP150_CSV),
  "smallcap" : ("smallcap.csv", SMALLCAP250_CSV)
}

def fetch_csv(url: str, retries: int = 3, sleep: float = 1.0) -> pd.DataFrame:
  last_err = None
  for attempt in range(1, retries + 1):
    try:
      resp = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
      resp.raise_for_status()
      content_bytes = resp.content
      text_data = content_bytes.decode("utf-8", errors="ignore")
      csv_buffer = io.StringIO(text_data)
      df = pd.read_csv(csv_buffer)
      if df.empty:
        raise ValueError("Empty CSV received from source")
      return df
    except Exception as e:
      last_err = e
      if attempt < retries:
        time.sleep(sleep)
      else:
        raise RuntimeError(f"Failed to fecth {url} : {last_err}") from last_err

def to_yahoo_symbol(sym: str) -> str:
  s = str(sym).strip().upper()
  if not s.endswith(".NS"):
    s += ".NS"
  return s

def extract_symbols(df : pd.DataFrame) -> List[str]:
  for col in ["Symbol", "SYMBOL", "symbol", "Ticker", "ticker"]:
    if col in df.columns:
      return [to_yahoo_symbol(x) for x in df[col].dropna().astype(str).tolist()]
  for col in df.columns:
    if df[col].dtype == object:
      try:
        if df[col].str.len().median() < 8:
          return [to_yahoo_symbol(x) for x in df[col].dropna().astype(str).tolist()]
      except Exception:
        continue
  raise ValueError("No suitabe ticker column found in CSV.")

def ensure_min_count(bucket: str, symbols: List[str], min_count: int = 100) -> List[str]:
  symbols = list(dict.fromkeys(symbols))
  if len(symbols) >= min_count:
    return symbols
  
  print(f"[i] {bucket}: {len(symbols)} < {min_count}, topping up from NIFTY 500")
  n500_df = fetch_csv(NIFTY500_CSV)
  n500_syms = extract_symbols(n500_df)
  remaining = [s for s in n500_syms if s not in symbols]

  caps = []
  for s in remaining:
    try:
      t = yf.Ticker(s)
      cap = getattr(t.fast_info, "market _cap", None)
      if cap is None:
        cap = t.info.get("marketCap")
      if cap:
        caps.append((s, cap))
    except Exception:
      continue

  caps.sort(key=lambda x:x[1],reverse=True)
  need = min_count - len(symbols)
  symbols.extend([s for s, _ in caps[:need]])
  return symbols

def build_bucket(name:str, url:str, out_file: str, min_count: int = 100) -> None:
  print(f"Building {name} from {url}")
  df = fetch_csv(url)
  syms = extract_symbols(df)
  syms = ensure_min_count(name, syms, min_count=min_count)

  os.makedirs(OUT_DIR, exist_ok=True)
  out_path = os.path.join(OUT_DIR, out_file)
  pd.DataFrame({"ticker": syms}).to_csv(out_path, index=False)

  print(f"Saved {len(syms)} tickers to {out_path}")
  
def parse_args():
    """
    Read command-line arguments.
    """
    parser = argparse.ArgumentParser(description="Build NSE market-cap buckets automatically.")
    parser.add_argument("--min", type=int, default=100, help="Minimum symbols per bucket")
    return parser.parse_args()

def main():
    """
    Main function: builds all buckets.
    """
    args = parse_args()
    for bucket, (fname, url) in TARGETS.items():
        build_bucket(bucket, url, fname, min_count=args.min)

if __name__ == "__main__":
    main()

