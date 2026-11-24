# Project UAS Pemrograman 3: Apache Airflow ETL & Monitoring Pipeline

**Mata Kuliah:** Praktikum Pemrograman 3  
**Program Studi:** D4 Sains Data Terapan — Politeknik Elektronika Negeri Surabaya (PENS)  
**Fokus Tim:** Kelompok 4 (Monitoring, Error Handling, & End-to-End ETL Orchestration)  
**Sumber Data:** Open-Meteo Weather Forecast API (Koordinat Surabaya: `-7.25, 112.75`)  

---

## 1. Deskripsi Proyek & Sumber Data
Proyek ini membangun otomatisasi *data pipeline* menggunakan **Apache Airflow** untuk menarik, membersihkan, menyimpan, dan memantau data prakiraan cuaca per jam di Surabaya secara terjadwal.

* **Sumber Data:** API publik [Open-Meteo Weather Forecast](https://open-meteo.com/) (`https://api.open-meteo.com/v1/forecast`) tanpa autentikasi API key.
* **Parameter Ekstraksi:** Koordinat Surabaya (`latitude=-7.25`, `longitude=112.75`), zona waktu `Asia/Jakarta` (WIB), rentang 1 hari (`forecast_days=1`) yang mencakup 4 variabel utama:
  * `temperature_2m` (Suhu udara dalam °C)
  * `relative_humidity_2m` (Kelembapan relatif dalam %)
  * `precipitation` (Curah hujan dalam mm)
  * `wind_speed_10m` (Kecepatan angin dalam km/jam)

---

## 2. Arsitektur Multi-DAG & Alur Kerja

```text
[Open-Meteo Weather API]
          │
          ▼
┌────────────────────────────────────────────────────────────────┐
│ DAG 1: dag_1_etl_cuaca_surabaya (Schedule: @daily / 0 0 * * *) │
│  1. extract_weather_api   ──▶ Ambil JSON mentah via requests   │
│  2. transform_weather_data ─▶ Validasi anomali & cleaning      │
│  3. load_to_storage       ──▶ Simpan inkremental ke CSV        │
│  4. trigger_dag_ringkasan ──▶ TriggerDagRunOperator            │
└───────────────────────────────┬────────────────────────────────┘
                                │ (Auto-Trigger Antar-DAG)
                                ▼
┌────────────────────────────────────────────────────────────────┐
│ DAG 2: dag_2_ringkasan_monitoring (Schedule: Triggered)        │
│  1. generate_daily_summary ─▶ Agregasi statistik cuaca harian  │
│       ├── [all_success] ──▶ pipeline_success_log               │
│       └── [one_failed]  ──▶ alert_error_handler                │
└────────────────────────────────────────────────────────────────┘
