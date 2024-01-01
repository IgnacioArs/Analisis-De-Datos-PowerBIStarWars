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
    prefix="/stars-wars/vehicles",
    tags=["stars-wars-vehicles-MS"]
);

@route.get("/getVehicles")
async def getVehicles():
    APIVEHICLES = os.getenv("APIVEHICLES");
    URI = str(APIVEHICLES);
    data = requests.get(URI);
    dataVehicles = data.json()
    vehiclesData = dataVehicles["results"];

    df = pd.DataFrame(vehiclesData);
    csv_data = df.to_csv(index=False,sep=";");
    response = Response(content=csv_data, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=species.csv"})
    return response;


@route.post("/subir-csv-gcp/vehicles")
async def subir_csv_gcp_vehicles():
    directory = 'C:\\Users\\Ignacio\\Downloads' 
    rename_csv_files(directory)  
    download_carpeta = 'C:\\Users\\Ignacio\\Downloads'
    archivos_en_descargas = os.listdir(download_carpeta)
    archivo_deseado = "vehicles.csv" if "vehicles.csv" in archivos_en_descargas else None

    if archivo_deseado in archivos_en_descargas:
        ruta_de_archivo = os.path.join(download_carpeta, archivo_deseado)
        df = pd.read_csv(ruta_de_archivo, sep=";")
        
        df['edited'] = df['edited'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ").isoformat())
        df['edited'] = df['edited'].str.replace('\.\d{3}Z', '', regex=True)

        def parse_created_datetime(x):
            try:
                # Intenta analizar con milisegundos
                return datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ")
            except ValueError:
                # Si falla, intenta analizar sin milisegundos
                return datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ")

        df['created'] = df['created'].apply(parse_created_datetime)

        # Desestructura los datos
        name = df["name"].tolist();
        model = df["model"].tolist();
        manufacturer = df["manufacturer"].tolist();
        cost_in_credits = df["cost_in_credits"].tolist();
        length = df["length"].tolist();
        max_atmosphering_speed = df["max_atmosphering_speed"].tolist();
        crew = df["crew"].tolist();
        passengers = df["passengers"].tolist();
        cargo_capacity = df["cargo_capacity"].tolist();
        consumables = df["consumables"].tolist();
        vehicle_class = df["vehicle_class"].tolist();
        pilots = df["pilots"].tolist();
        films = df["films"].tolist();
        created = df["created"].tolist();
        edited = df["edited"].tolist();
        url = df["url"].tolist();

        # Crea un diccionario con los datos desestructurados
        data_dict = {
            "name":name,
            "model":model,
            "manufacturer":manufacturer,
            "cost_in_credits":cost_in_credits,
            "length":length,
            "max_atmosphering_speed":max_atmosphering_speed,
            "crew":crew,
            "passengers":passengers,
            "cargo_capacity":cargo_capacity,
            "consumables":consumables,
            "vehicle_class":vehicle_class,
            "pilots":pilots,
            "films":films,
            "created":created,
            "edited":edited,
            "url":url
        }

        # Insertar datos en BigQuery
        client = bigquery.Client()
        
        # Define la referencia a la tabla donde deseas insertar los datos
        dataset_id = 'stars_wars'  # Reemplaza con tu ID de conjunto de datos
        table_id = 'stars_wars_vehicles'  # Reemplaza con tu ID de tabla
        table_ref = client.dataset(dataset_id).table(table_id)

        # Convierte los datos a una lista de diccionarios
        rows_to_insert = []
        for i in range(len(data_dict["name"])):
            row = {
                "name": data_dict["name"][i],
                "model":data_dict["model"][i],
                "manufacturer":data_dict["manufacturer"][i],
                "cost_in_credits":data_dict["cost_in_credits"][i],
                "length":data_dict["length"][i],
                "max_atmosphering_speed":data_dict["max_atmosphering_speed"][i],
                "crew":data_dict["crew"][i],
                "passengers":data_dict["passengers"][i],
                "cargo_capacity":data_dict["cargo_capacity"][i],
                "consumables":data_dict["consumables"][i],
                "vehicle_class":data_dict["vehicle_class"][i],
                "pilots":data_dict["pilots"][i],
                "films":data_dict["films"][i],
                "created":data_dict["created"][i],
                "edited":data_dict["edited"][i],
                "url":data_dict["url"][i],
            }

            rows_to_insert.append(row)

        schema = [
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("model", "STRING"),
            bigquery.SchemaField("manufacturer", "STRING"),
            bigquery.SchemaField("cost_in_credits", "STRING"),
            bigquery.SchemaField("length", "FLOAT"),
            bigquery.SchemaField("max_atmosphering_speed", "INTEGER"),
            bigquery.SchemaField("crew", "INTEGER"),
            bigquery.SchemaField("passengers", "INTEGER"),
            bigquery.SchemaField("cargo_capacity", "STRING"),
            bigquery.SchemaField("consumables", "STRING"),
            bigquery.SchemaField("vehicle_class", "STRING"),
            bigquery.SchemaField("pilots", "STRING"),
            bigquery.SchemaField("films", "STRING"),
            bigquery.SchemaField("created", "DATETIME"),
            bigquery.SchemaField("edited", "DATETIME"),
            bigquery.SchemaField("url", "STRING")
        ]

        # Inserta los datos en la tabla
        errors = client.insert_rows(table_ref, rows_to_insert, selected_fields=schema)

        if errors:
            print(f"Errores al insertar datos en BigQuery: {errors}")
        else:
            os.remove(ruta_de_archivo)
            print("Datos insertados con éxito en BigQuery.")

        return df.to_json()
    else:
        # El archivo no se encuentra
        return {"error": "El archivo deseado no se encuentra en la carpeta de descargas."}


def rename_csv_files(directory):

    file_list = os.listdir(directory)

    for filename in file_list:
        if filename.lower().endswith(".csv"):
         
            source_file = os.path.join(directory, filename)
            destination_file = os.path.join(directory, "vehicles.csv")

    
            if os.path.exists(destination_file):
                print("El archivo 'vehicles.csv' ya existe en la carpeta.")
                return

        
            os.rename(source_file, destination_file)

    print("Se han renombrado todos los archivos CSV en la carpeta a 'vehicles.csv'.")

