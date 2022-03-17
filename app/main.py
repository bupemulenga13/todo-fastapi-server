from sys import prefix
from middleware.views import *
from fastapi import FastAPI
from typing import Any, List
from fastapi.middleware.cors import CORSMiddleware

from scripts.dsa import appointments
from .routers import (
    appointments,
    dashboard,
    diagnostics,
    inidicators,
    labour_and_delivery,
    labs,
    medications,
    morbidity,
    referals,
    testing,
    visits,
    vitals
)

app = FastAPI() 

origins = [
    "http://localhost:3000",
    "http://localhost:3001",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(appointments.router)
app.include_router(dashboard.router)
app.include_router(diagnostics.router)
app.include_router(inidicators.router)
app.include_router(labour_and_delivery.router)
app.include_router(labs.router)
app.include_router(medications.router)
app.include_router(morbidity.router)
app.include_router(referals.router)
app.include_router(testing.router)
app.include_router(vitals.router)
app.include_router(visits.router)

@app.get('/api')
async def home():
    """Root"""
    return {"message": "Buchi Billionaire"}


