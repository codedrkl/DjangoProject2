# ES Options Quant Platform

A professional Django-based **ES futures options analysis and trade suggestion engine** built for serious options traders.

Built with love and precision by Grok + you.

---

## Features

### Core
- Real-time EOD data from **Databento** (GLBX.MDP3)
- Full **Black-76 Greeks**: Delta, Gamma, Theta, Implied Volatility
- Dynamic strike range (automatically adjusts around current ES future)
- Beautiful collapsible option chain with sticky headers
- **Outcome View** – Intelligent trade suggestions with real interactive PnL graphs (Plotly)

### Visuals & UX
- Dark TOS-inspired theme
- RR Ratio, Breakeven, Edge, Probability on every suggestion
- Real payoff diagrams for Bull Put Spreads, Iron Condors, Bear Call Spreads, etc.

### Coming Soon
- Automatic nightly scheduler (2 AM AEST)
- Dynamic trade suggestions generated from today’s actual chain
- Alert system (“Break a leg”, early closure, hedging recommendations)
- Daily Narrative Summary + Weekly Review (Sentinel)
- Gamma Territory & Gamma Flip detection
- IV vs Realised Vol comparison
- Platform-agnostic execution layer (TOS / IBKR / TastyTrade)

---

## Tech Stack

- **Backend**: Django 5+
- **Database**: PostgreSQL
- **Data**: Databento (OPRA/GLBX)
- **Greeks**: Black-76 model with scipy
- **Charts**: Plotly
- **Frontend**: Tailwind CSS + Alpine (minimal)

---

## Setup & Run

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Setup database
python manage.py makemigrations
python manage.py migrate

# 3. Run the main chain
python manage.py download_es_eod

# 4. Start server
python manage.py runserver