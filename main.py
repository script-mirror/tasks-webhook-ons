import uvicorn
from fastapi import FastAPI
from app.controller import router as webhook_router

app = FastAPI()
app.include_router(webhook_router, prefix="/api", tags=["webhook"])


def main() -> None:
    uvicorn.run(app, port=8000, host='0.0.0.0')

if __name__ == "__main__":
    main()