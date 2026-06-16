# Elasticsearch DB Cloner

Clone Elasticsearch indices, mappings, settings, aliases, and documents from source to target.

## Setup

```bash
npm install
```

Create `.env`:

```env
SOURCE_ELASTICSEARCH_HOST=https://source-elasticsearch.example.com
SOURCE_ELASTICSEARCH_USERNAME=source_username
SOURCE_ELASTICSEARCH_PASSWORD=source_password
SOURCE_ELASTICSEARCH_TLS_REJECT_UNAUTHORIZED=true

TARGET_ELASTICSEARCH_CLOUD_ID=target_cloud_id
TARGET_ELASTICSEARCH_API_KEY=target_api_key
TARGET_ELASTICSEARCH_TLS_REJECT_UNAUTHORIZED=true
```

## Dry Run

Check source data only. No target DB required.

```bash
npm run dry-run
```

## Clone

Run after target DB is ready.

```bash
npm run clone -- --yes
```
