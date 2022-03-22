from sys import prefix
from middleware.dsa_views import *
from middleware.dqa_views import *
from fastapi import FastAPI
from typing import Any, List
from fastapi.middleware.cors import CORSMiddleware

from .routers import (
	facility_details,
	hts,
	interactions,
	pharm_pick,
	pmtct,
	tx
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


app.include_router(facility_details.router)
app.include_router(hts.router)
app.include_router(interactions.router)
app.include_router(pharm_pick.router)
app.include_router(pmtct.router)
app.include_router(tx.router)





@app.get('/api')
async def home():
    """Root"""
    return {"message": "Buchi Billionaire"}


