# Air Quality IoT Server

FastAPI server for ESP32 air quality monitoring.

## Endpoints
- POST /api/data
- GET /latest
- GET /download/csv
- GET /health

## Run locally
uvicorn main:app --reload
