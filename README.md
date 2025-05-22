# Apache Kafka

Sebuah perusahaan logistik mengelola beberapa gudang penyimpanan yang menyimpan barang sensitif seperti makanan, obat-obatan, dan elektronik. Untuk menjaga kualitas penyimpanan, gudang-gudang tersebut dilengkapi dengan dua jenis sensor:
- Sensor Suhu
- Sensor Kelembaban

Sensor akan mengirimkan data setiap detik. Perusahaan ingin memantau kondisi gudang secara real-time untuk mencegah kerusakan barang akibat suhu terlalu tinggi atau kelembaban berlebih.

# Struktur Folder
```
Apache-kafka_5027231050/
├── .venv/
├── producers/
│   ├── sproducer_suhu.py
│   └── producer_kelembaban.py
├── consumer/
│   └── consumer_join.py
├── requirements.txt
└── README.md
```

# Soal
## 1. Topik Kafka
Buat dua topik di Apache Kafka:
- sensor-suhu-gudang
- sensor-kelembaban-gudang

Topik ini akan digunakan untuk menerima data dari masing-masing sensor secara real-time.

Untuk membuat topiknya, buka command prompt dan pastikan berada di folder kafka yang sudah kita download dan jalankan menggunakan command berikut.
```
.\bin\windows\kafka-topics.bat --create --topic sensor-suhu-gudang --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
.\bin\windows\kafka-topics.bat --create --topic sensor-kelembaban-gudang --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## 2. Simulasikan Data Sensor (Producer Kafka)
Sebelum menjalankan producer kafka pastikan bahwa zookeeper dan kafka servernya sudah berjalan. Untuk menjalankannya buka dua terminal di Command Prompt, satu untuk zookeeper dan satu lagi untuk kafka servernya. Pastikan menjalankannya di folder kafka yang sudah kita download. Jalankan menggunakan command berikut.
```
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
.\bin\windows\kafka-server-start.bat .\config\server.properties
```
![Screenshot 2025-05-22 195158](https://github.com/user-attachments/assets/02f7edb8-3bdd-4aca-b719-d02c7b41dfbe)
![Screenshot 2025-05-22 195212](https://github.com/user-attachments/assets/8fa97ecb-b0dd-40ee-a569-1696f869ba7c)

a. Producer Suhu
- Kirim data setiap detik
- Format:

{"gudang_id": "G1", "suhu": 82}

Untuk menjalankan producer suhu dapat membuka terminal di VSCode (atau terminal lain). Pastikan virtual environment (.venv) aktif di terminal tersebut.
```
(.venv) PS D:\Apache-Kafka_5027231050> python producers/producer_suhu.py
```
![Screenshot 2025-05-22 192631](https://github.com/user-attachments/assets/5ca12abb-d5e5-481a-b521-2f7fcc4da516)

b. Producer Kelembaban
- Kirim data setiap detik
- Format:

{"gudang_id": "G1", "kelembaban": 75}

Untuk menjalankan producer kelembaban dapat membuka terminal baru di VSCode (atau terminal lain). Pastikan virtual environment (.venv) aktif di terminal tersebut.
```
(.venv) PS D:\Apache-Kafka_5027231050> python producers/producer_kelembaban.py
```
![Screenshot 2025-05-22 192643](https://github.com/user-attachments/assets/763bcbb0-947f-4272-815a-c6afeaaef9ef)

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

Contoh Output Gabungan:

[PERINGATAN KRITIS]

Gudang G1:
- Suhu: 84°C
- Kelembaban: 73%
- Status: Bahaya tinggi! Barang berisiko rusak

Gudang G2:
- Suhu: 78°C
- Kelembaban: 68%
- Status: Aman

Gudang G3:
- Suhu: 85°C
- Kelembaban: 65%
- Status: Suhu tinggi, kelembaban normal

Gudang G4:
- Suhu: 79°C
- Kelembaban: 75%
- Status: Kelembaban tinggi, suhu aman
