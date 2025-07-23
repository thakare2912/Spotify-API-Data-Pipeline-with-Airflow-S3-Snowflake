# Spotify ETL Data Pipeline

This project is an **Apache Airflow pipeline** that extracts playlist track data from the **Spotify API**, stores raw JSON data in **Amazon S3**, processes it into normalized CSVs for **albums**, **artists**, and **songs**, uploads the processed CSVs back to S3, and finally moves the raw data to an archive folder. You can also load the transformed data to **Snowflake** automatically using **Snowpipe**.

---

##  Project Structure


---

## Pipeline Steps

 **1️) Fetch Spotify Playlist Data**  
Uses the Spotify Web API and **Spotipy** to pull tracks from a playlist.

 **2️)Store Raw Data to S3**  
Uploads raw JSON files to `raw_data/to_processed/`.

 **3️) Read Raw Data from S3**  
Reads JSON files from the raw folder.

 **4️)Process Albums, Artists, Songs**  
Normalizes JSON to structured CSVs:
- `album_id`, `name`, `total_tracks`, `url`
- `artist_id`, `artist_name`, `external_url`
- `song_id`, `song_name`, `duration_ms`, `url`, `popularity`, `song_added`, `album_id`, `artist_id`

 **5️)Store Processed CSVs to S3**  
Uploads CSVs to:
- `transformed_data/album_data/`
- `transformed_data/artist_data/`
- `transformed_data/songs_data/`

 **6️) Move Processed Raw Files**  
Moves raw JSON from `raw_data/to_processed/` to `raw_data/processed/`.

 **7️)Load to Snowflake**  
You can configure **Snowpipe** to auto-ingest the processed CSVs to your Snowflake tables.

---

##  Technologies Used

- **Apache Airflow**
- **Spotipy**
- **AWS S3**
- **Python**
- **Pandas**
- **Snowflake**
- **Snowpipe**

---


