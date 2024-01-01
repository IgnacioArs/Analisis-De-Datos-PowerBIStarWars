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
    prefix="/stars-wars/species",
    tags=["stars-wars-species-MS"]
)



@route.get("/getSpecies")
async def getSpecies():
    APISPECIES = os.getenv("APISPECIES");
    URI = str(APISPECIES);
    data = requests.get(URI);
    dataSpecies = data.json();
    speciesData = dataSpecies["results"]

    contador= 0;
    arraySpecies = [];
    while contador < len(speciesData):
          objeto = speciesData[contador];
          arraySpecies.append(objeto);
          contador += 1;
    df = pd.DataFrame(arraySpecies);
    csv_data = df.to_csv(index=False,sep=";");
    response = Response(content=csv_data, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=species.csv"})

    return response     


@route.post("/subir-csv-gcp/species")
async def subir_csv_gcp_species():
    directory = 'C:\\Users\\Ignacio\\Downloads' 
    rename_csv_files(directory)  
    download_carpeta = 'C:\\Users\\Ignacio\\Downloads'
    archivos_en_descargas = os.listdir(download_carpeta)
    archivo_deseado = "species.csv" if "species.csv" in archivos_en_descargas else None

    if archivo_deseado in archivos_en_descargas:
        ruta_de_archivo = os.path.join(download_carpeta, archivo_deseado)
        df = pd.read_csv(ruta_de_archivo, sep=";")
        
        df['edited'] = df['edited'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ").isoformat())
        df['edited'] = df['edited'].str.replace('\.\d{3}Z', '', regex=True)

        df['created'] = df['created'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ").isoformat())
        df['created'] = df['created'].str.replace('\.\d{3}Z', '', regex=True)

        
        name = df["name"].tolist();
        classification = df["classification"].tolist();
        designation = df["designation"].tolist();
        average_height = df["average_height"].tolist();
        skin_colors = df["skin_colors"].tolist();
        hair_colors = df["hair_colors"].tolist();
        eye_colors = df["eye_colors"].tolist();
        average_lifespan = df["average_lifespan"].tolist();
        homeworld = df["homeworld"].tolist();
        language = df["language"].tolist();
        people = df["people"].tolist();
        films = df["films"].tolist();
        created = df["created"].tolist();
        edited = df["edited"].tolist();
        url = df["url"].tolist();

        
        data_dict = {
            "name": name,
            "classification":classification,
            "designation":designation,
            "average_height":average_height,
            "skin_colors": skin_colors,
            "hair_colors":hair_colors,
            "eye_colors":eye_colors,
            "average_lifespan": average_lifespan,
            "homeworld": homeworld,
            "language": language,
            "people":people,
            "films": films,
            "created": created,
            "edited": edited,
            "url": url
        }

        
        client = bigquery.Client()
        
        
        dataset_id = 'stars_wars'  
        table_id = 'stars_wars_species'  
        table_ref = client.dataset(dataset_id).table(table_id)

        
        rows_to_insert = []
        for i in range(len(data_dict["name"])):
            row = {
                "name": data_dict["name"][i],
                "classification":data_dict["classification"][i],
                "designation":data_dict["designation"][i],
                "average_height":str(data_dict["average_height"][i]),
                "skin_colors":str(data_dict["skin_colors"][i]),
                "hair_colors":str(data_dict["hair_colors"][i]),
                "eye_colors":str(data_dict["eye_colors"][i]),
                "average_lifespan":data_dict["average_lifespan"][i],
                "homeworld":str(data_dict["homeworld"][i]),
                "language":str(data_dict["language"][i]),
                "people":data_dict["people"][i],
                "films": data_dict["films"][i],
                "created": data_dict["created"][i],
                "edited": data_dict["edited"][i],
                "url": data_dict["url"][i]
            }

            rows_to_insert.append(row)

        schema = [
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("classification", "STRING"),
            bigquery.SchemaField("designation", "STRING"),
            bigquery.SchemaField("average_height", "STRING"),
            bigquery.SchemaField("skin_colors", "STRING"),
            bigquery.SchemaField("hair_colors", "STRING"),
            bigquery.SchemaField("eye_colors", "STRING"),
            bigquery.SchemaField("average_lifespan", "STRING"),
            bigquery.SchemaField("homeworld", "STRING"),
            bigquery.SchemaField("language", "STRING"),
            bigquery.SchemaField("people", "STRING"),
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
            destination_file = os.path.join(directory, "species.csv")

            
            if os.path.exists(destination_file):
                print("El archivo 'species.csv' ya existe en la carpeta.")
                return

           
            os.rename(source_file, destination_file)

    print("Se han renombrado todos los archivos CSV en la carpeta a 'species.csv'.")
