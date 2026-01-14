name: Scrape Parfumo (Top Lists)

on:
  workflow_dispatch:
    inputs:
      start_url:
        description: "URL del listado (ej: https://www.parfumo.com/Perfumes/Tops/Men)"
        required: false
        default: "https://www.parfumo.com/Perfumes/Tops/Men"
      max_pages:
        description: "Cuantas paginas intentar"
        required: false
        default: "5"
      limit:
        description: "Cuantos perfumes maximo guardar"
        required: false
        default: "20"
  schedule:
    - cron: "0 */12 * * *"  # cada 12h UTC (puedes cambiarlo luego)

jobs:
  scrape:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Run scraper
        env:
          DATABASE_URL: ${{ secrets.DATABASE_URL }}
          START_URL: ${{ github.event.inputs.start_url || 'https://www.parfumo.com/Perfumes/Tops/Men' }}
          MAX_PAGES: ${{ github.event.inputs.max_pages || '5' }}
          LIMIT: ${{ github.event.inputs.limit || '20' }}
        run: python scrape_parfumo.py
