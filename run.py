import uvicorn
from app.main import app

if __name__ == "__main__":
    print("Starting Stock Finder API...")
    print("Swagger UI: http://localhost:8000/docs")
    print("ReDoc: http://localhost:8000/redoc")
    print("API Base URL: http://localhost:8000")
    print("=" * 60)
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
