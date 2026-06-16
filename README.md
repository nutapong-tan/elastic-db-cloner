# Elasticsearch DB Cloner

โปรเจกต์นี้ใช้ clone Elasticsearch จาก cluster เก่าไป cluster ใหม่ โดยคัดลอก:

- index settings ที่จำเป็น
- mappings
- aliases
- documents ทั้งหมดพร้อม `_id` และ routing

ค่าเริ่มต้นจะ clone เฉพาะ non-system indices เช่น `users`, `merchants`, `roles` และจะข้าม index ที่ขึ้นต้นด้วย `.` เพื่อไม่ไปยุ่งกับ system/security indices ของ Elastic Cloud

## วิธีใช้

1. ติดตั้ง dependency

```bash
npm install
```

2. ใส่ credential ในไฟล์ `.env` ที่สร้างไว้ให้แล้ว

```env
SOURCE_ELASTICSEARCH_HOST=https://xpoker-els.ingsolutions.co
SOURCE_ELASTICSEARCH_USERNAME=admin
SOURCE_ELASTICSEARCH_PASSWORD=your_source_password
SOURCE_ELASTICSEARCH_TLS_REJECT_UNAUTHORIZED=true

TARGET_ELASTICSEARCH_CLOUD_ID=your_cloud_id
TARGET_ELASTICSEARCH_API_KEY=your_api_key
TARGET_ELASTICSEARCH_TLS_REJECT_UNAUTHORIZED=true
```

ค่า default จะ clone ทุก open index ที่ไม่ใช่ system index อยู่แล้ว จึงไม่ต้องใส่ `INDEX_ALLOWLIST=*` ใน `.env`

3. ตรวจฝั่ง source ก่อน clone

```bash
npm run dry-run
```

คำสั่งนี้จะอ่าน settings, mappings, aliases และ scroll documents ทุก batch จาก source เพื่อเช็คว่าอ่านค่าได้ครบและสร้าง bulk payload ได้ โดยไม่ต้องมี target DB และไม่เขียนข้อมูลใด ๆ

4. Clone จริงหลังจากตั้งค่า target พร้อมแล้ว

```bash
npm run clone -- --yes
```

## หมายเหตุ

- `TARGET_ELASTICSEARCH_CLOUD_ID` ต้องเป็น Cloud ID จริงจาก Elastic Cloud ในรูปแบบ `deployment-name:base64...` ถ้าเป็น URL ให้ใช้ `TARGET_ELASTICSEARCH_HOST` แทน
- ถ้า source และ target เป็น cluster เดียวกัน และไม่ได้ตั้ง `TARGET_INDEX_PREFIX` หรือ `TARGET_INDEX_SUFFIX` สคริปต์จะหยุดเพื่อกันการเขียนทับตัวเอง
- สคริปต์ไม่ได้ clone Elasticsearch users/roles/API keys, snapshots, ingest pipelines, ILM policies หรือ index templates โดยอัตโนมัติ เพราะบน Elastic Cloud มักต้องจัดการแยก
- ถ้า index ใดปิด `_source` ไว้ สคริปต์จะ clone documents ของ index นั้นไม่ได้ เพราะ Elasticsearch ไม่ส่ง document body กลับมาให้
# elastic-db-cloner
