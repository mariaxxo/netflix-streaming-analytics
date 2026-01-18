# netflix-streaming-analytics

Zespół: Maria Bielawa, Milena Dziepak
Temat: Strumieniowa analiza preferencji użytkowników platformy Netflix

---Technologie---
Ingest: Azure Event Hubs
Przetwarzanie strumieniowe: Databricks Structured Streaming (Spark)
Warstwa serwująca: Delta Lake w Azure Data Lake Storage Gen2
IaC: Terraform
CI/CD: Github actions
Dashboard: PowerBI

--Instrukcja--
1. Lokalne testy w języku Python
   python -m venv venv
   
   venv\Scripts\activate
   
   pip install -r requirements.txt

   python /notebooks/streaming_job.py
   
   python /event-producer/producer.py

3. W chmurze
   cd iac
   terraform init
   terraform apply
   -skonfigurować zmienne środowiskowe-
   -uruchomić databricks-
   -otworzyć tabele wynikowe w PowerBI-

--Sekrety--
Wszystkie connection strings i klucze pochodzą z ustawień -> zasady dostępu współdzielonego

