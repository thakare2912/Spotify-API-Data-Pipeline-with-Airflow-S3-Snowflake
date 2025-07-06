# 🎵 Spotify End-to-End Data Pipeline using Apache Airflow, AWS & Snowflake

![Spotify End-To-End Data Pipeline Project using airfleo](https://github.com/user-attachments/assets/a506bd49-14d4-43bd-a4be-34d80c41ca46)


## 📌 Project Overview

This project demonstrates an **end-to-end ETL data pipeline** that extracts data from the **Spotify API**, stores raw and processed data in **Amazon S3**, and loads transformed data into **Snowflake** using **Snowpipe**, all orchestrated with **Apache Airflow** running in **Docker**.

---

## ⚙️ **Architecture**

- **Source:** Spotify API  
- **Ingestion:** Python + Airflow tasks  
- **Storage:** Amazon S3 (raw + transformed data)  
- **Processing:** Python (Pandas) transformations  
- **Orchestration:** Apache Airflow on Docker  
- **Data Warehouse:** Snowflake (loaded via Snowpipe)

**🔗 Note:** In the diagram, **Power BI** is not used — analytics can be done directly in Snowflake or with other BI tools if needed.

---

## 🗂️ **Key Steps**

1️⃣ **Extract Data:**  
Fetch playlist data from the Spotify API using Spotipy and store raw JSON in Amazon S3.

2️⃣ **Transform Data:**  
Read raw JSON files from S3, process albums, artists, and songs data into clean CSVs using Pandas.

3️⃣ **Store Transformed Data:**  
Save transformed CSVs back to S3 in organized folders (`album_data`, `artist_data`, `songs_data`).

4️⃣ **Load into Snowflake:**  
Use Snowpipe to automatically ingest the transformed data from S3 into Snowflake for analysis.

5️⃣ **Automation:**  
All tasks are orchestrated with Apache Airflow running inside Docker **on your local machine**.

---

## 🐍 **Key Technologies**

- **Python**, Spotipy, Pandas
- **Apache Airflow**
- **Docker**
- **AWS S3**
- **Snowflake**, Snowpipe

---


## 🚀 **How to Run**

1️⃣ **Set up AWS S3 & Snowflake:**  
- Create an S3 bucket (`spotify-etl-project-hanumant`) with `raw_data` and `transformed_data` folders.  
- Configure Snowpipe to watch your S3 bucket.

2️⃣ **Deploy Airflow with Docker:**  
- Use `docker-compose.yml` to spin up Airflow **locally** with Docker.

3️⃣ **Set Secrets:**  
- Store Spotify API credentials in Airflow Variables (`spotify_client_id` & `spotify_client_secret`).  
- Configure AWS connection (`aws_s3_airbnb`).

4️⃣ **Run the DAG:**  
- Trigger the `spotify_data_pipeline` DAG from the Airflow UI.

5️⃣ **Check Outputs:**  
- Raw JSON files in `raw_data/to_processed/`  
- Clean CSVs in `transformed_data/`  
- Loaded data in Snowflake tables.

---

## 🔑 **Key Learnings**

✅ Building ETL pipelines with **API extraction**  
✅ Orchestrating workflows with **Apache Airflow**  
✅ Managing data storage on **AWS S3**  
✅ Automating loading into **Snowflake** using **Snowpipe**  
✅ Running Airflow on **Docker**

---



