from . import models
from .database import engine, get_db
from .routes import topology, chargeback
from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
models.Base.metadata.create_all(bind=engine)


app = FastAPI(title="EC DPS Chargeback")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(chargeback.router, tags=["Chargeback"])
app.include_router(topology.router, tags=["Topology"])

