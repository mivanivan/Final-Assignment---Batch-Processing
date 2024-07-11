# Final Assignment  - RFM Analysis Batch Processing

## Tujuan Proyek
Proyek ini bertujuan untuk membentuk data pipeline untuk mengotomasi proses penarikan data dari Database, pemrosesan data menggunakan sckit-learn, dan mengembalikannya ke database awal untuk di analisis. Proyek ini akan membantu para data Analyst yang berkewajiban melakukan pembaharuan secara periodik terhadap tabel segmentasi RFM. Proyek ini akan mengurangi tenaga dalam proses pembaharuan dan mengurangi human error dalam prosesnya.

## Struktur Direktori
Berikut adalah struktur direktori proyek ini:

```
project-root/
├── airflow
│   └── dags/
│   │    └── 2_final_assignment.py
│   ├──logs/
│   ├──plugins/
│   ├──resources/
│   ├──scripts/
│   ├──airflow
│   ├──docker-compose.yaml
│   ├──Dockerfile
│   └──prometheus
├── metabase
│   └── docker-compose.yaml
├── mysql
│   └── docker-compose.yaml
├── PPTX
└── README
```

## Prasyarat
Pastikan Anda telah menginstal Docker dan Docker Compose sebelum menjalankan proyek ini.

## Instruksi Menjalankan Proyek

### 1. Install mysql:
#### a. Masuk kedalam folder mysql/
#### b. jalankan command docker-compose up -d

### 2. Install Airflow:
#### a. Masuk kedalam folder airflow/
#### b. jalankan command docker-compose up -d
#### c. buka http://localhost:8080 di browser

### 3. Install Airflow:
#### a. Masuk kedalam folder metabase/
#### b. jalankan command docker-compose up -d
#### c. buka http://localhost:3000 di browser

## Penjelasan Kode

### `2_final_assignment.py`
File ini mendefinisikan DAG Airflow yang digunakan untuk melakukan processing terhadap data hingga mengeluarkan output berupa user-user yang telah di segmen berdasarkan *recency*, *frequency*, dan *monetary*

1. **etl_mysql_to_mysql_2**: Define Airflow DAG named 
2. **Extract**: take data from MySQL into xcom
3. **install_dependencies**: Installing necessary package such as scikit-learn 
4. **transform_data**: processing data from raw data into users segmentation
5. **load_to_mysql**: load the data back to MySQL

Output analisis ini dicetak ke konsol dan disimpan kembali ke tabel mysql dan divisualisasikan di metabase.
1. **Monthly Revenue**: menampilkan pergerakan penjualan tiap bulan
2. **Monthly Active User**: menampilkan pergerakan jumlah active user tiap bulan
3. **Percentage RFM Segmentation**: menampikan jumlah user yang menjadi champion, potential, lost, dan sleep