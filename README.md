# Best Assets

Gradio app that returns the top-performing creative assets (images, videos, mixed_media) for a given industry, use case, platform, device and context — ranked by NIS.

## 🔗 Live service

**https://best-assets-sql-gradio-473867468713.us-central1.run.app**

Deployed on Cloud Run (`best-assets-sql-gradio`, project `neurons-development`, region `us-central1`).
To confirm the current URL:

```bash
gcloud run services describe best-assets-sql-gradio \
  --project=neurons-development --region=us-central1 --format='value(status.url)'
```

## Run locally

```bash
cd gui_sql
just run          # or: python main.py  → http://localhost:8080
```

## Deploy

```bash
cd gui_sql
just deploy       # builds the image and deploys to Cloud Run
```

## Layout

| Path | What it is |
| --- | --- |
| [gui_sql/](gui_sql/) | Current app — reads assets from Cloud SQL |
| [gui/](gui/) | Previous version — reads assets from CSVs in GCS |
| [sql_table/](sql_table/) | Scripts that loaded the CSV data into Cloud SQL |
| [prep_data.ipynb](prep_data.ipynb) | Data preparation notebook |
