from db import models
from db.database import engine
from fastapi import FastAPI
from router import quantum

app = FastAPI()

list_router = [
    quantum.router
]

for router in list_router: 
    app.include_router(router) 

models.Base.metadata.create_all(bind=engine)
