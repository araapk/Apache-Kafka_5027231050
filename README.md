# Apache-Kafka

Sebuah perusahaan logistik mengelola beberapa gudang penyimpanan yang menyimpan barang sensitif seperti makanan, obat-obatan, dan elektronik. Untuk menjaga kualitas penyimpanan, gudang-gudang tersebut dilengkapi dengan dua jenis sensor:
- Sensor Suhu
- Sensor Kelembaban

Sensor akan mengirimkan data setiap detik. Perusahaan ingin memantau kondisi gudang secara real-time untuk mencegah kerusakan barang akibat suhu terlalu tinggi atau kelembaban berlebih.

# Struktur Folder
```
Apache-Kafka_5027231050/
├── .venv/ # Folder virtual environment (diabaikan oleh Git)
├── producers/ # Folder berisi producer Kafka
│ ├── producer_suhu.py # Mengirim data suhu ke Kafka
│ └── producer_kelembaban.py # Mengirim data kelembaban ke Kafka
├── consumer/ # Folder berisi consumer menggunakan PySpark
│ └── consumer_join.py # Memproses data dari Kafka menggunakan PySpark
└── requirements.txt # File daftar dependensi Python
```

# Soal
## 1. Topik Kafka
Untuk membuat topik kafka, buka command prompt dan masuk ke folder kafka hasil downloadan kita lalu jalankan command berikut.

```
.\bin\windows\kafka-topics.bat --create --topic sensor-suhu-gudang --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
.\bin\windows\kafka-topics.bat --create --topic sensor-kelembaban-gudang --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## 2. Simulasikan Data Sensor (Producer Kafka)
Sebelum menjalankan producer suhu dan kelembaban pastikan bahwa zookeeper dan kafka servernya sudah berjalan di folder kafka download. Untuk menjalankannya gunakan command berikut di command prompt. Buka dua terminal, satu untuk menjalankan zookeeper dan satunya untuk kafka servernya.

```
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
.\bin\windows\kafka-server-start.bat .\config\server.properties
```
![Screenshot 2025-05-22 195158](https://github.com/user-attachments/assets/8e25506d-1b73-4558-a794-e0e92aad4e7d)
![Screenshot 2025-05-22 195212](https://github.com/user-attachments/assets/9106b781-3c3b-4249-bf28-131d4b8e5ef3)

a. Producer Suhu
- Kirim data setiap detik
- Format:

{"gudang_id": "G1", "suhu": 82}

Untuk menjalankannya buka terminal di VSCode (atau terminal lain). Pastikan virtual environment (.venv) aktif di terminal tersebut.

```
(.venv) PS D:\Apache-Kafka_5027231050> python producers/producer_suhu.py 
```
![Screenshot 2025-05-22 192631](https://github.com/user-attachments/assets/78112965-3ba0-484a-92dc-0f525fcae416)

b. Producer Kelembaban
- Kirim data setiap detik
- Format:

{"gudang_id": "G1", "kelembaban": 75}

Untuk menjalankannya buka terminal baru di VSCode (atau terminal lain). Pastikan virtual environment (.venv) aktif di terminal tersebut.

```
(.venv) PS D:\Apache-Kafka_5027231050> python producers/producer_kelembaban.py
```
![Screenshot 2025-05-22 192643](https://github.com/user-attachments/assets/25de5593-e99a-4d0a-8756-20faeb987f5c)

## 3. Konsumsi dan Olah Data dengan PySpark
a. Buat PySpark Consumer

Konsumsi data dari kedua topik Kafka.

b. Lakukan Filtering
- Suhu > 80°C → tampilkan sebagai peringatan suhu tinggi
- Kelembaban > 70% → tampilkan sebagai peringatan kelembaban tinggi

Contoh Output:

[Peringatan Suhu Tinggi]

Gudang G2: Suhu 85°C

[Peringatan Kelembaban Tinggi]

Gudang G3: Kelembaban 74%

## 4. Gabungkan Stream dari Dua Sensor

Lakukan join antar dua stream berdasarkan `gudang_id` dan `window` waktu (misalnya 10 detik) untuk mendeteksi kondisi bahaya ganda.

c. Buat Peringatan Gabungan

Jika ditemukan suhu > 80°C dan kelembaban > 70% pada gudang yang sama, tampilkan peringatan kritis.
