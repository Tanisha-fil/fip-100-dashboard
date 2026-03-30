# FIP-100 Dashboard ⛽

🔗 **Live Site: [tanisha-katara.github.io/fip-100-dashboard](https://tanisha-katara.github.io/fip-100-dashboard/)**
Data and charts to track [FIP 100](https://github.com/filecoin-project/FIPs/blob/master/FIPS/fip-0100.md) real-world network behavior.

## ⚙️ Development

Adding new charts to this dashboard is easy. You'll need Node and Python installed.

1. Set the required environment variable.
2. Run `make data` to write the datasets JSON into `public/`.
3. Install the dashboard dependencies with `make setup`, then `make dev` for the dev server.

You should be able to open the dashboard locally at [`http://localhost:4321/fip-100-dashboard`](http://localhost:4321/fip-100-dashboard).

Then, adding a chart is easy.

1. Write a dataset script under `datasets/` that outputs a JSON to `public/<name>.json`
2. Run `make data` to run it and store the JSON
3. Import it in `src/pages/index.astro`
4. Render a new chart via `<LineChart data={...} x="date" y="value" label="My Metric" />`.

### 🔑 Environment Variables

- `ENCODED_GOOGLE_APPLICATION_CREDENTIALS` is a base64-encoded GCP service account JSON.

## 📚 Resources

- [FIP-100](https://github.com/filecoin-project/FIPs/blob/master/FIPS/fip-0100.md)
- [Discussion](https://github.com/filecoin-project/FIPs/discussions/1105)
- [Timelines](https://github.com/filecoin-project/community/discussions/74)
- [Filecoin in Numbers](https://numbers.filecoindataportal.xyz/)