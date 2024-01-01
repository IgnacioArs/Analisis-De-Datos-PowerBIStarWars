from fastapi import APIRouter
import pandas as pd
from datetime import datetime
from fastapi.responses import Response
from google.cloud import bigquery
import os
from dotenv import  load_dotenv
import requests


load_dotenv();
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r'C:\Users\Ignacio\Desktop\powerBIStarWars\stars-wars-402204-d46ab06a83b3.json'

route = APIRouter(
    prefix="/stars-wars/planets",
    tags=["stars-wars-planets-MS"]
)

@route.get("/getPlanets")
async def getPlanets():
    APIPLANETS = os.getenv("APIPLANETS");
    URI = str(APIPLANETS);
    data = requests.get(URI);
    planets = data.json(); 
    data =planets["results"];
    dataPlanets = data;
    contador = 0;
    arrayPlanets = [];
    while contador < len(dataPlanets):
          payload = dataPlanets[contador];
          arrayPlanets.append(payload);   
          contador += 1;
    
    csv = pd.DataFrame(arrayPlanets);
    csv_data   = csv.to_csv(index=False, sep=";");

    response = Response(content=csv_data, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=planets.csv"})

    return response


@route.post("/subir-csv-gcp/planets")
async def subir_csv_gcp_planets():
    directory = 'C:\\Users\\Ignacio\\Downloads' 
    rename_csv_files(directory)  
    download_carpeta = 'C:\\Users\\Ignacio\\Downloads'
    archivos_en_descargas = os.listdir(download_carpeta)
    archivo_deseado = "planets.csv" if "planets.csv" in archivos_en_descargas else None

    if archivo_deseado in archivos_en_descargas:
        ruta_de_archivo = os.path.join(download_carpeta, archivo_deseado)
        df = pd.read_csv(ruta_de_archivo, sep=";")
        
        df['edited'] = df['edited'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ").isoformat())
        df['edited'] = df['edited'].str.replace('\.\d{3}Z', '', regex=True)

        df['created'] = df['created'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ").isoformat())
        df['created'] = df['created'].str.replace('\.\d{3}Z', '', regex=True)

     
        names = df["name"].tolist()
        rotation_periods = df["rotation_period"].tolist()
        orbital_periods = df["orbital_period"].tolist()
        diameters = df["diameter"].tolist()
        climates = df["climate"].tolist()
        gravitys = df["gravity"].tolist()
        terrains = df["terrain"].tolist()
        surface_waters = df["surface_water"].tolist()
        populations = df["population"].tolist()
        residents = df["residents"].tolist()
        films = df["films"].tolist()
        created = df['created'] = df['created'].str[:-1]
        edited = df["edited"].tolist()
        urls = df["url"].tolist()

  
        data_dict = {
            "name": names,
            "rotation_period": rotation_periods,
            "orbital_period":orbital_periods,
            "diameter": diameters,
            "climate": climates,
            "gravity": gravitys,
            "terrain": terrains,
            "surface_water": surface_waters,
            "population": populations,
            "residents": residents,
            "films": films,
            "created": created,
            "edited": edited,
            "url": urls
        }

      
        client = bigquery.Client()
        
        
        dataset_id = 'stars_wars'  
        table_id = 'stars_wars_planets'  
        table_ref = client.dataset(dataset_id).table(table_id)

       
        rows_to_insert = []
        for i in range(len(data_dict["name"])):
            row = {
                "name": data_dict["name"][i],
                "rotation_period": data_dict["rotation_period"][i],
                "orbital_period": data_dict["orbital_period"][i],
                "diameter":data_dict["diameter"][i],
                "climate":data_dict["climate"][i],
                "gravity":str(data_dict["gravity"][i]),
                "terrain":data_dict["terrain"][i],
                "surface_water":data_dict["surface_water"][i],
                "population":data_dict["population"][i],
                "residents":data_dict["residents"][i],
                "films": data_dict["films"][i],
                "created": data_dict["created"][i],
                "edited": data_dict["edited"][i],
                "url": data_dict["url"][i]
            }

            rows_to_insert.append(row)

        schema = [
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("rotation_period", "INTEGER"),
            bigquery.SchemaField("orbital_period", "INTEGER"),
            bigquery.SchemaField("diameter", "INTEGER"),
            bigquery.SchemaField("climate", "STRING"),
            bigquery.SchemaField("gravity", "STRING"),
            bigquery.SchemaField("terrain", "STRING"),
            bigquery.SchemaField("surface_water", "STRING"),
            bigquery.SchemaField("population", "STRING"),
            bigquery.SchemaField("residents", "STRING"),
            bigquery.SchemaField("films", "STRING"),
            bigquery.SchemaField("created", "DATETIME"),
            bigquery.SchemaField("edited", "DATETIME"),
            bigquery.SchemaField("url", "STRING")
        ]

        
        errors = client.insert_rows(table_ref, rows_to_insert, selected_fields=schema)

        if errors:
            print(f"Errores al insertar datos en BigQuery: {errors}")
        else:
            os.remove(ruta_de_archivo)
            print("Datos insertados con éxito en BigQuery.")

        return df.to_json()
    else:
        
        return {"error": "El archivo deseado no se encuentra en la carpeta de descargas."}



def rename_csv_files(directory):
  
    file_list = os.listdir(directory)

    for filename in file_list:
        if filename.lower().endswith(".csv"):
            
            source_file = os.path.join(directory, filename)
            destination_file = os.path.join(directory, "planets.csv")

           
            if os.path.exists(destination_file):
                print("El archivo 'planets.csv' ya existe en la carpeta.")
                return

       
            os.rename(source_file, destination_file)

    print("Se han renombrado todos los archivos CSV en la carpeta a 'planets.csv'.")