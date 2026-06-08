from __future__ import annotations

import csv
import json
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class Config:
    benchmark_symbol: str = "^NDX"
    yahoo_screener_url: str = "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved"
    yahoo_spark_url: str = "https://query1.finance.yahoo.com/v7/finance/spark"
    range_: str = "3mo"
    interval: str = "1d"
    chunk_size: int = 25
    timeout_seconds: int = 30
    data_dir: Path = Path("data")


CFG = Config()
STATE_LOCK = threading.Lock()
STATE = {
    "updating": False,
    "last_updated": None,
    "error": None,
    "rows": [],
}


def yahoo_json(url: str, params: dict[str, object], timeout_seconds: int) -> dict:
    request = Request(
        f"{url}?{urlencode(params, doseq=True)}",
        headers={
            "Accept": "application/json,text/plain,*/*",
            "Accept-Language": "en-US,en;q=0.9",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0 Safari/537.36"
            ),
        },
    )
    with urlopen(request, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8", errors="replace"))


def fetch_nasdaq100_symbols(cfg: Config = CFG) -> list[str]:
    payload = yahoo_json(
        cfg.yahoo_screener_url,
        {"scrIds": "most_actives_ndx", "count": 250},
        cfg.timeout_seconds,
    )
    results = payload.get("finance", {}).get("result", [])
    if not results:
        raise RuntimeError("Yahoo screener did not return Nasdaq 100 data.")
    quotes = results[0].get("quotes", [])
    symbols = sorted({quote.get("symbol") for quote in quotes if quote.get("symbol")})
    if len(symbols) < 90:
        raise RuntimeError(f"Only {len(symbols)} symbols returned; expected about 100.")
    return symbols


def chunked(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def parse_spark_payload(payload: dict) -> dict[str, pd.DataFrame]:
    parsed: dict[str, pd.DataFrame] = {}
    for item in payload.get("spark", {}).get("result", []):
        symbol = item.get("symbol")
        responses = item.get("response", [])
        if not symbol or not responses:
            continue
        response = responses[0]
        timestamps = response.get("timestamp") or []
        quote_blocks = response.get("indicators", {}).get("quote", [])
        if not timestamps or not quote_blocks:
            continue
        closes = quote_blocks[0].get("close", [])
        df = pd.DataFrame(
            {
                "date": pd.to_datetime(timestamps, unit="s", utc=True).tz_convert(None).normalize(),
                "close": pd.to_numeric(pd.Series(closes), errors="coerce"),
            }
        )
        df = df.dropna(subset=["close"]).sort_values("date").drop_duplicates("date").reset_index(drop=True)
        if len(df) >= 21:
            parsed[symbol] = df
    return parsed


def fetch_spark_prices(symbols: list[str], cfg: Config = CFG) -> dict[str, pd.DataFrame]:
    result: dict[str, pd.DataFrame] = {}
    for group in chunked(symbols, cfg.chunk_size):
        try:
            payload = yahoo_json(
                cfg.yahoo_spark_url,
                {"symbols": ",".join(group), "range": cfg.range_, "interval": cfg.interval},
                cfg.timeout_seconds,
            )
            result.update(parse_spark_payload(payload))
        except Exception:
            for symbol in group:
                payload = yahoo_json(
                    cfg.yahoo_spark_url,
                    {"symbols": symbol, "range": cfg.range_, "interval": cfg.interval},
                    cfg.timeout_seconds,
                )
                result.update(parse_spark_payload(payload))
                time.sleep(0.05)
    return result


def seven_day_return(close: pd.Series) -> float:
    if len(close) < 8:
        return np.nan
    return float(close.iloc[-1] / close.iloc[-8] - 1)


def score_ma(close: pd.Series) -> tuple[int, int, int, int]:
    price = float(close.iloc[-1])
    ma5 = float(close.tail(5).mean())
    ma10 = float(close.tail(10).mean())
    ma20 = float(close.tail(20).mean())
    above_ma5 = int(price > ma5)
    above_ma10 = int(price > ma10)
    above_ma20 = int(price > ma20)
    return above_ma5 + above_ma10 + above_ma20, above_ma5, above_ma10, above_ma20


def build_ranking() -> list[dict]:
    symbols = fetch_nasdaq100_symbols()
    all_symbols = symbols + [CFG.benchmark_symbol]
    prices = fetch_spark_prices(all_symbols)

    benchmark = prices.get(CFG.benchmark_symbol)
    if benchmark is None:
        raise RuntimeError("Missing ^NDX benchmark price data.")
    ndx_ret7 = seven_day_return(benchmark["close"])
    if not np.isfinite(ndx_ret7):
        raise RuntimeError("Cannot calculate Nasdaq 100 seven-trading-day return.")

    rows = []
    for symbol in symbols:
        df = prices.get(symbol)
        if df is None or len(df) < 21:
            continue
        close = df["close"]
        stock_ret7 = seven_day_return(close)
        if not np.isfinite(stock_ret7):
            continue
        score, above_ma5, above_ma10, above_ma20 = score_ma(close)
        relative_ratio = (1 + stock_ret7) / (1 + ndx_ret7) - 1
        rows.append(
            {
                "排名": 0,
                "股票代码": symbol,
                "价格": round(float(close.iloc[-1]), 2),
                "评分": score,
                "高于MA5": above_ma5,
                "高于MA10": above_ma10,
                "高于MA20": above_ma20,
                "个股7日涨跌幅": stock_ret7,
                "纳斯达克100指数7日涨跌幅": ndx_ret7,
                "相对纳斯达克100七日涨跌幅比例": relative_ratio,
                "最新日期": df["date"].iloc[-1].strftime("%Y-%m-%d"),
            }
        )

    rows.sort(key=lambda x: (x["评分"], x["相对纳斯达克100七日涨跌幅比例"]), reverse=True)
    for i, row in enumerate(rows, start=1):
        row["排名"] = i
    return rows


def save_latest(rows: list[dict]) -> None:
    CFG.data_dir.mkdir(parents=True, exist_ok=True)
    json_file = CFG.data_dir / "latest.json"
    csv_file = CFG.data_dir / "latest.csv"
    payload = {"last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "rows": rows}
    json_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    if rows:
        with csv_file.open("w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            writer.writerows(rows)


def load_latest() -> None:
    json_file = CFG.data_dir / "latest.json"
    if not json_file.exists():
        return
    try:
        payload = json.loads(json_file.read_text(encoding="utf-8"))
    except Exception:
        return
    with STATE_LOCK:
        STATE["last_updated"] = payload.get("last_updated")
        STATE["rows"] = payload.get("rows") or []
        STATE["error"] = None


def run_update() -> dict:
    with STATE_LOCK:
        if STATE["updating"]:
            return {"ok": False, "message": "更新正在进行中，请稍后刷新。"}
        STATE["updating"] = True
        STATE["error"] = None
    try:
        rows = build_ranking()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with STATE_LOCK:
            STATE["rows"] = rows
            STATE["last_updated"] = now
            STATE["error"] = None
        save_latest(rows)
        return {"ok": True, "message": "更新完成", "last_updated": now, "rows": rows}
    except Exception as exc:
        with STATE_LOCK:
            STATE["error"] = str(exc)
        return {"ok": False, "message": str(exc)}
    finally:
        with STATE_LOCK:
            STATE["updating"] = False


def start_update_thread() -> dict:
    with STATE_LOCK:
        if STATE["updating"]:
            return {"ok": False, "message": "更新正在进行中，请稍后刷新。"}

    thread = threading.Thread(target=run_update, daemon=True)
    thread.start()
    return {"ok": True, "message": "已开始更新，请稍候。"}


HTML = r"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>纳斯达克100相对强弱排名</title>
  <style>
    :root { color-scheme: light; --ink:#172033; --muted:#667085; --line:#d9dee8; --fill:#f6f8fb; --brand:#155eef; }
    body { margin:0; font-family: Arial, "Microsoft YaHei", sans-serif; color:var(--ink); background:#fff; }
    header { padding:28px 28px 16px; border-bottom:1px solid var(--line); }
    h1 { margin:0 0 8px; font-size:26px; line-height:1.25; }
    .sub { color:var(--muted); font-size:14px; line-height:1.6; max-width:980px; }
    main { padding:20px 28px 36px; }
    .toolbar { display:flex; gap:12px; align-items:center; flex-wrap:wrap; margin-bottom:16px; }
    button { border:0; background:var(--brand); color:#fff; font-size:14px; padding:10px 16px; border-radius:6px; cursor:pointer; }
    button:disabled { opacity:.65; cursor:not-allowed; }
    .status { color:var(--muted); font-size:14px; }
    .table-wrap { overflow:auto; border:1px solid var(--line); border-radius:8px; }
    table { width:100%; border-collapse:collapse; font-size:13px; white-space:nowrap; }
    th, td { padding:9px 10px; border-bottom:1px solid var(--line); text-align:right; }
    th { background:var(--fill); color:#344054; position:sticky; top:0; z-index:1; }
    th:nth-child(2), td:nth-child(2) { text-align:left; font-weight:700; }
    tr:hover td { background:#f9fbff; }
    .score { font-weight:700; }
    .pos { color:#067647; }
    .neg { color:#b42318; }
    .note { margin-top:14px; color:var(--muted); font-size:13px; line-height:1.55; }
  </style>
</head>
<body>
  <header>
    <h1>纳斯达克100成分股相对强弱排名</h1>
    <div class="sub">
      排序规则：先按 MA5、MA10、MA20 评分排序，价格高于每条均线得 1 分，最高 3 分；
      同评分股票再按相对纳斯达克100指数的 7 个交易日涨跌幅比例排序。
    </div>
  </header>
  <main>
    <div class="toolbar">
      <button id="updateBtn">更新排名</button>
      <span id="status" class="status">正在读取...</span>
    </div>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>排名</th><th>股票代码</th><th>价格</th><th>评分</th>
            <th>高于MA5</th><th>高于MA10</th><th>高于MA20</th>
            <th>个股7日涨跌幅</th><th>NDX 7日涨跌幅</th><th>相对比例</th><th>最新日期</th>
          </tr>
        </thead>
        <tbody id="tbody"></tbody>
      </table>
    </div>
    <div class="note">
      数据来自 Yahoo Finance 免费接口，可能存在延迟。点击“更新排名”会在服务器端重新运行 Python 排名代码。
    </div>
  </main>
  <script>
    const statusEl = document.getElementById("status");
    const tbody = document.getElementById("tbody");
    const btn = document.getElementById("updateBtn");
    const pct = v => Number.isFinite(v) ? (v * 100).toFixed(2) + "%" : "N/A";
    const cls = v => v > 0 ? "pos" : (v < 0 ? "neg" : "");

    function render(data) {
      statusEl.textContent = data.updating ? "更新中..." : `最近更新：${data.last_updated || "暂无"}`;
      if (data.error) statusEl.textContent += `；错误：${data.error}`;
      tbody.innerHTML = "";
      (data.rows || []).forEach(row => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${row["排名"]}</td>
          <td>${row["股票代码"]}</td>
          <td>${Number(row["价格"]).toFixed(2)}</td>
          <td class="score">${row["评分"]}</td>
          <td>${row["高于MA5"]}</td>
          <td>${row["高于MA10"]}</td>
          <td>${row["高于MA20"]}</td>
          <td class="${cls(row["个股7日涨跌幅"])}">${pct(row["个股7日涨跌幅"])}</td>
          <td class="${cls(row["纳斯达克100指数7日涨跌幅"])}">${pct(row["纳斯达克100指数7日涨跌幅"])}</td>
          <td class="${cls(row["相对纳斯达克100七日涨跌幅比例"])}">${pct(row["相对纳斯达克100七日涨跌幅比例"])}</td>
          <td>${row["最新日期"]}</td>`;
        tbody.appendChild(tr);
      });
    }

    async function loadData() {
      const res = await fetch("/api/ranking");
      render(await res.json());
    }

    async function updateRanking() {
      btn.disabled = true;
      statusEl.textContent = "更新中，通常需要几十秒...";
      const res = await fetch("/api/update", { method: "POST" });
      render(await res.json());
      const timer = setInterval(async () => {
        const res = await fetch("/api/ranking");
        const data = await res.json();
        render(data);
        if (!data.updating) {
          clearInterval(timer);
          btn.disabled = false;
        }
      }, 2500);
    }

    btn.addEventListener("click", updateRanking);
    loadData();
  </script>
</body>
</html>"""


class Handler(BaseHTTPRequestHandler):
    def send_json(self, payload: dict, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/" or self.path.startswith("/?"):
            body = HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if self.path == "/api/ranking":
            with STATE_LOCK:
                self.send_json(dict(STATE))
            return
        if self.path == "/download.csv":
            csv_file = CFG.data_dir / "latest.csv"
            if not csv_file.exists():
                self.send_response(404)
                self.end_headers()
                return
            body = csv_file.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", "text/csv; charset=utf-8")
            self.send_header("Content-Disposition", 'attachment; filename="nasdaq100-ranking.csv"')
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_response(404)
        self.end_headers()

    def do_POST(self) -> None:
        if self.path == "/api/update":
            result = start_update_thread()
            with STATE_LOCK:
                payload = dict(STATE)
            payload.update(result)
            self.send_json(payload)
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, fmt: str, *args) -> None:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {fmt % args}")


def main() -> None:
    load_latest()
    port = int(os.environ.get("PORT", "8000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"Serving on http://127.0.0.1:{port}")
    server.serve_forever()


if __name__ == "__main__":
    main()
