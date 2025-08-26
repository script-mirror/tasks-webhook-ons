import uvicorn
from fastapi import FastAPI, Depends
from app.controller import router as webhook_router
from app.dependencies import cognito
from fastapi.security import HTTPBearer
from dotenv import load_dotenv

load_dotenv()

auth_scheme = HTTPBearer()

app = FastAPI(
    title="API Tasks webhook ONS",
    docs_url="/tasks/api/docs",
    redoc_url="/tasks/api/redoc",
    openapi_url="/tasks/api/openapi.json",
    description="API para receber webhooks da ONS e disparar workflows correspondentes."
)
app.include_router(
    webhook_router, prefix="/tasks/api", tags=["webhook"],
    dependencies=[Depends(auth_scheme),
    Depends(cognito.auth_required)]
)


@app.get("/tasks/api/health")
def health():
    return {"status": "ok"}


def main() -> None:
    uvicorn.run(app, port=8000, host='0.0.0.0')

if __name__ == "__main__":
    main()