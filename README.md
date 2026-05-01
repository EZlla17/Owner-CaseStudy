# Owner Sales Intelligence App

This project analyzes restaurant sales-call data and restaurant web/search signals, then powers a dashboard for sales managers.

## Deployed App

You can access the deployed app here:

- Deployed app: `https://<your-deployed-link>`

## Project Structure

- `owner-dashboard/` – Next.js dashboard UI
- `situations-moments/`
  - `scripts/` – moment extraction and clustering scripts
  - `data/` – situation/moment outputs
- `restaurants/`
  - `scripts/` – restaurant enrichment and fit-score scripts
  - `data/` – restaurant enrichment outputs
- `restaurant.csv` – base restaurant input
- `call-transcript.csv` – base call transcript input

## Requirements

- Node.js 18+ (recommended: Node 20+)
- npm
- Python 3.10+

## Run the Dashboard Locally

```bash
cd owner-dashboard
npm install
npm run dev
```

Open:

- `http://localhost:3000`
