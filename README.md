# Portfolio Tracker

A self-hosted Flask web app for tracking a personal stock portfolio: holdings, cost basis, live prices (via `yfinance`), limit orders, watchlists, earnings/macro events, valuation inputs, an equity curve, and portfolio-level risk metrics (beta, Sharpe).

State is stored as plain JSON on disk (`portfolio.json`, `equity_log.json`). No accounts, no database.

---

## Live demo

A public demo instance runs at: _add your Render URL here after first deploy_

The demo starts with a 3-ETF sample portfolio. Add your own holdings through the UI to overwrite it.

---

## Running locally

```bash
git clone https://github.com/Michaelxia1009/Michael-s-Portfolio-Tracker.git
cd Michael-s-Portfolio-Tracker
pip install -r requirements.txt
python app.py
```

Open http://127.0.0.1:5000.

Data is written to `portfolio.json` and `equity_log.json` in the project directory. Both are gitignored.

---

## Deploying to Render (recommended)

Render gives you a persistent disk so the JSON files survive restarts and redeploys.

1. Fork this repo (or use it directly if you own it).
2. Sign in at https://render.com and connect your GitHub account.
3. Click **New → Blueprint**, select this repo, and hit **Apply**. Render reads [`render.yaml`](render.yaml) and provisions:
   - A Python web service (Starter plan, ~$7/mo).
   - A 1 GB persistent disk mounted at `/var/data`.
   - The `DATA_DIR=/var/data` env var so the app writes there.
4. First build takes ~3 min. Once it's live, open the URL Render shows you.

### Configuration reference

| Env var | Purpose | Default |
| --- | --- | --- |
| `DATA_DIR` | Directory where `portfolio.json` and `equity_log.json` are stored. Must be writable. | Project directory |
| `UPSTASH_REDIS_REST_URL` | Optional — if set, state is stored in Upstash Redis instead of local files. | unset |
| `UPSTASH_REDIS_REST_TOKEN` | Token for Upstash Redis. Required if `UPSTASH_REDIS_REST_URL` is set. | unset |
| `PORT` | Port the app binds to. Set automatically by Render. | `5000` (local) |

---

## Deploying elsewhere

Any host that runs a long-lived Python process with a writable directory works. Typical start command:

```bash
gunicorn app:app --bind 0.0.0.0:$PORT --workers 2 --timeout 120
```

Set `DATA_DIR` to a directory that persists across restarts.

**Vercel / AWS Lambda / serverless hosts won't work out of the box** — the filesystem is read-only and ephemeral. Use Redis (see env vars above) or move to a host with a real disk.

---

## Privacy note

`portfolio.json` and `equity_log.json` contain your actual financial data. They are listed in `.gitignore` and should never be committed. On Render (and similar hosts) they live on a private persistent disk scoped to your service. Double-check before forking or making this repo public with your own data in it.

---

## License

MIT
