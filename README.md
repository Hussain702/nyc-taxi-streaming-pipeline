# NYC Taxi Streaming Pipeline 🚕📊

A real-time data engineering project that streams **NYC Taxi Trip data** from a producer into **Kafka**, processes it with **PyFlink**, and stores the transformed results into **PostgreSQL** for downstream analytics.  

---

## 📌 Project Overview
This project simulates a **real-time streaming pipeline**:
1. **Producer (Python)**  
   Reads NYC Taxi trip data (Parquet/CSV) and sends events into **Kafka**.
2. **Kafka**  
   Acts as the **message broker** to handle real-time event streams.
3. **PyFlink**  
   Consumes events from Kafka, applies simple **transformations**, and prepares data for storage.
4. **PostgreSQL**  
   Stores processed trip records for analytics, BI dashboards, and reporting.

---

## 🏗️ Architecture

```mermaid
flowchart TD
    A[NYC Taxi Data<br>(CSV / Parquet)] -->|Producer.py| B[Kafka]
    B --> C[PyFlink<br>Stream Processing]
    C --> D[PostgreSQL<br>Taxi Events Table]
    D --> E[BI / Analytics Tools]
**
