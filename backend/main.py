from fastapi import FastAPI
from api.routes import router as api_router
from core.data_manager import data_manager
import os
import time

app = FastAPI(title="BlinkQuant Node")

app.include_router(api_router)

@app.on_event("startup")
async def startup_event():
    print(f"Booting Node {os.getenv('NODE_INDEX')} at {time.ctime()}...")
    data_manager.load_data()

@app.get("/")
def index():
    return {"message": "BlinkQuant Online", "node": os.getenv("NODE_INDEX")}
