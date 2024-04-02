from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
from quantum import call_api

app = FastAPI()

class AnyData(BaseModel):
    pass

@app.post("/receive-json/")
def receive_json(background_tasks: BackgroundTasks, data: AnyData):
    background_tasks.add_task(call_api)
    return {"message": "Task completed"}