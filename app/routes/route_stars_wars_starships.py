import pandas as pd
from fastapi import APIRouter
from fastapi.responses import StreamingResponse,Response
from datetime import datetime
from google.cloud import bigquery
import os
from dotenv import load_dotenv
import requests

load_dotenv();
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r'C:\Users\Ignacio\Desktop\powerBIStarWars\stars-wars-402204-d46ab06a83b3.json'

route = APIRouter(
    prefix="/stars-wars/starchips",
    tags=["stars-wars-starchips-MS"]
);


@route.get("/getStarchips")
async def getStarchips():
    APISTARSHIPS = os.getenv("APISTARSHIPS");
    URI = str(APISTARSHIPS);
    data = requests.get(URI);
    dataStarchips = data.json();
    starchipsData = dataStarchips["results"];

    df = pd.DataFrame(starchipsData);
    csv_data = df.to_csv(index=False,sep=";");

    response = Response(content=csv_data, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=starchips.csv"})
    return response;


@route.post("/subir-csv-gcp/starchips")
async def subir_csv_gcp_starchips():
    directorio = "C:\\Users\\Ignacio\\Downloads"
    rename_csv_files(directorio)
    download_carpetas = "C:\\Users\\Ignacio\\Downloads"
    archivos_de_descargas = os.listdir(download_carpetas)
    archivo_deseado = "starchips.csv" if "starchips.csv" in archivos_de_descargas else None

    if archivo_deseado in archivos_de_descargas:
        ruta_de_archivo = os.path.join(download_carpetas, archivo_deseado)
        df = pd.read_csv(ruta_de_archivo, sep=";")

       
        df['max_atmosphering_speed'] = df['max_atmosphering_speed'].apply(lambda x: str(x) if not pd.isna(x) else "")
        df['crew'] = df['crew'].str.replace(',', '.').astype(str)
        df['edited'] = df['edited'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ").isoformat())
        df['edited'] = df['edited'].str.replace('\.\d{3}Z', '', regex=True)

        def parse_created_datetime(x):
            try:
               
                return datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ")
            except ValueError:
                
                return datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ")

        df['created'] = df['created'].apply(parse_created_datetime)

        diccionario_de_datos = {
            "name": df["name"].tolist(),
            "model": df["model"].tolist(),
            "manufacturer": df["manufacturer"].tolist(),
            "cost_in_credits": df["cost_in_credits"].tolist(),
            "length": df["length"].tolist(),
            "max_atmosphering_speed": df["max_atmosphering_speed"].tolist(),
            "crew": df["crew"].tolist(),
            "passengers": df["passengers"].tolist(),
            "cargo_capacity": df["cargo_capacity"].tolist(),
            "consumables": df["consumables"].tolist(),
            "hyperdrive_rating": df["hyperdrive_rating"].tolist(),
            "MGLT": df["MGLT"].tolist(),
            "starship_class": df["starship_class"].tolist(),
            "pilots": df["pilots"].tolist(),
            "films": df["films"].tolist(),
            "created": df["created"].tolist(),
            "edited": df["edited"].tolist(),
            "url": df["url"].tolist()
        }

        cliente = bigquery.Client()
        dataset_id = "stars_wars"
        table_id = "stars_wars_starchips"
        table_ref = cliente.dataset(dataset_id).table(table_id)

        schema = [
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("model", "STRING"),
            bigquery.SchemaField("manufacturer", "STRING"),
            bigquery.SchemaField("cost_in_credits", "STRING"),
            bigquery.SchemaField("length", "STRING"),
            bigquery.SchemaField("max_atmosphering_speed", "STRING"),
            bigquery.SchemaField("crew", "STRING"),
            bigquery.SchemaField("passengers", "STRING"),
            bigquery.SchemaField("cargo_capacity", "STRING"),
            bigquery.SchemaField("consumables", "STRING"),
            bigquery.SchemaField("hyperdrive_rating", "FLOAT"),
            bigquery.SchemaField("MGLT", "INTEGER"),
            bigquery.SchemaField("starship_class", "STRING"),
            bigquery.SchemaField("pilots", "STRING"),
            bigquery.SchemaField("films", "STRING"),
            bigquery.SchemaField("created", "DATETIME"),
            bigquery.SchemaField("edited", "DATETIME"),
            bigquery.SchemaField("url", "STRING")
        ]

        insertar_filas = []
        for i in range(len(diccionario_de_datos["name"])):
            rows = {
                "name": diccionario_de_datos["name"][i],
                "model": diccionario_de_datos["model"][i],
                "manufacturer": diccionario_de_datos["manufacturer"][i],
                "cost_in_credits": diccionario_de_datos["cost_in_credits"][i],
                "length": diccionario_de_datos["length"][i],
                "max_atmosphering_speed": diccionario_de_datos["max_atmosphering_speed"][i],
                "crew": str(diccionario_de_datos["crew"][i]),
                "passengers": str(diccionario_de_datos["passengers"][i]),
                "cargo_capacity": diccionario_de_datos["cargo_capacity"][i],
                "consumables": diccionario_de_datos["consumables"][i],
                "hyperdrive_rating": diccionario_de_datos["hyperdrive_rating"][i],
                "MGLT": diccionario_de_datos["MGLT"][i],
                "starship_class": diccionario_de_datos["starship_class"][i],
                "pilots": diccionario_de_datos["pilots"][i],
                "films": diccionario_de_datos["films"][i],
                "created": diccionario_de_datos["created"][i],
                "edited": diccionario_de_datos["edited"][i],
                "url": diccionario_de_datos["url"][i]
            }
            insertar_filas.append(rows)

        
        errors = cliente.insert_rows(table_ref, insertar_filas, selected_fields=schema)

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
            destination_file = os.path.join(directory, "starchips.csv")

    
            if os.path.exists(destination_file):
                print("El archivo 'starchips.csv' ya existe en la carpeta.")
                return

        
            os.rename(source_file, destination_file)

    print("Se han renombrado todos los archivos CSV en la carpeta a 'starchips.csv'.")