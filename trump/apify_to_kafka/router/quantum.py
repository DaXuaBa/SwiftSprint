from fastapi import APIRouter, Depends, BackgroundTasks
from db.data_processing import *
from db.database import get_db
from pydantic import BaseModel

router = APIRouter()

class AnyData(BaseModel):
    pass

@router.post("/receive-json/")
def receive_json(background_tasks: BackgroundTasks, data: AnyData, db: Session = Depends(get_db)):
    background_tasks.add_task(call_api, db)
    return {"message": "Task completed"}