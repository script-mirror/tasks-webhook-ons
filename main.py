import uvicorn
from fastapi import FastAPI
from app.controller import router as webhook_router
from dotenv import load_dotenv
load_dotenv()

app = FastAPI(
    title="API Tasks webhook ONS",
)
app.include_router(webhook_router, prefix="/tasks/api", tags=["webhook"])


@app.get("/tasks/api/health")
def health():
    return {"status": "ok"}


def main() -> None:
    uvicorn.run(app, port=8000, host='0.0.0.0')

if __name__ == "__main__":
    main()