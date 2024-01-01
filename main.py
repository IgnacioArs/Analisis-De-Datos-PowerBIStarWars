from fastapi import FastAPI
import uvicorn
import os
from dotenv import load_dotenv
from app.routes import route_stars_wars_peoples,route_stars_wars_planets,route_stars_wars_films,route_stars_wars_species,route_stars_wars_vehicles,route_stars_wars_starships

load_dotenv();
app = FastAPI(
    title="Mi API Star Wars POWERBI",
    description="Consumo Api Stars Wars para analisis de datos",
    version="1.0.0",
    openapi_url="/openapi.json",  # URL para la especificación OpenAPI
    docs_url="/docs/swagger"
)

app.include_router(route_stars_wars_peoples.route);
app.include_router(route_stars_wars_planets.route);
app.include_router(route_stars_wars_films.route);
app.include_router(route_stars_wars_species.route);
app.include_router(route_stars_wars_vehicles.route);
app.include_router(route_stars_wars_starships.route);

@app.get("/home")
async def init():
    PORT = os.getenv("PORT")
    return "Server Running on PORT: "+PORT

if __name__=="__main__":
    uvicorn.run(app);