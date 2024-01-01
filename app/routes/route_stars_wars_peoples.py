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
    prefix="/stars-wars/peoples",
    tags=["stars-wars-peoples-MS"]
)


@route.get('/getApis')
async def obtenerApis():
    API = os.getenv("API");
    URI = str(API);
    response = requests.get(URI);
    data = response.json();
    people =data["people"]; 
    planets = data["planets"];
    films = data["films"];
    species = data["species"];
    vehicles = data["vehicles"];
    starships = data["starships"];
    arrayApis = [people,planets,films,species,vehicles,starships]; 
    apisObtenidas = []

    for api in arrayApis:
        apisObtenidas.append(api);
 

    df = pd.DataFrame({'nombre_api': apisObtenidas})
    

    csv_data = df.to_csv(index=False,sep=";")

    return StreamingResponse(
        iter([csv_data]),
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=data.csv"}
    )
   


@route.get("/getPeoples")
async def getPeoples():
    APIPEOPLE = os.getenv("APIPEOPLE")
    URI = str(APIPEOPLE);
    response = requests.get(URI);
    data = response.json();
    dataPeople = data["results"]

    PeopleData = dataPeople;

    contador = 0

    people_data = [];

    while contador < len(PeopleData):
        objeto = PeopleData[contador]

   
        people_data.append(objeto)
        contador += 1


    df = pd.DataFrame(people_data)


    csv_data = df.to_csv(index=False, sep=";")
    response = Response(content=csv_data, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=peoples.csv"})


    return response



@route.post("/subir-csv-gcp/peoples")
async def subir_csv_gcp_peoples():
    directory = 'C:\\Users\\Ignacio\\Downloads'  
    rename_csv_files(directory)  
    download_carpeta = 'C:\\Users\\Ignacio\\Downloads'
    archivos_en_descargas = os.listdir(download_carpeta)
    archivo_deseado = "peoples.csv" if "peoples.csv" in archivos_en_descargas else None

    if archivo_deseado in archivos_en_descargas:
        ruta_de_archivo = os.path.join(download_carpeta, archivo_deseado)
        df = pd.read_csv(ruta_de_archivo, sep=";")
        
        df['edited'] = df['edited'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ").isoformat())
        df['edited'] = df['edited'].str.replace('\.\d{3}Z', '', regex=True)

        df['created'] = df['created'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ").isoformat())
        df['created'] = df['created'].str.replace('\.\d{3}Z', '', regex=True)


        names = df["name"].tolist()
        heights = df["height"].tolist()
        masses = df["mass"].tolist()
        hair_colors = df["hair_color"].tolist()
        skin_colors = df["skin_color"].tolist()
        eye_colors = df["eye_color"].tolist()
        birth_years = df["birth_year"].tolist()
        genders = df["gender"].tolist()
        homeworlds = df["homeworld"].tolist()
        films = df["films"].tolist()
        species = df["species"].tolist()
        vehicles = df["vehicles"].tolist()
        starships = df["starships"].tolist()
        created = df['created'] = df['created'].str[:-1]
        edited = df["edited"].tolist()
        urls = df["url"].tolist()

       
        data_dict = {
            "name": names,
            "height": heights,
            "mass": masses,
            "hair_color": hair_colors,
            "skin_color": skin_colors,
            "eye_color": eye_colors,
            "birth_year": birth_years,
            "gender": genders,
            "homeworld": homeworlds,
            "films": films,
            "species": species,
            "vehicles": vehicles,
            "starships": starships,
            "created": created,
            "edited": edited,
            "url": urls
        }

   
        client = bigquery.Client()
        
 
        dataset_id = 'stars_wars' 
        table_id = 'stars_wars_peoples' 
        table_ref = client.dataset(dataset_id).table(table_id)


        rows_to_insert = []
        for i in range(len(data_dict["name"])):
            row = {
                "name": data_dict["name"][i],
                "height": data_dict["height"][i],
                "mass": data_dict["mass"][i],
                "hair_color": str(data_dict["hair_color"][i]),
                "skin_color": str(data_dict["skin_color"][i]),
                "eye_color": data_dict["eye_color"][i],
                "birth_year": data_dict["birth_year"][i],
                "gender": str(data_dict["gender"][i]),
                "homeworld": str(data_dict["homeworld"][i]),
                "films": data_dict["films"][i],
                "species": data_dict["species"][i],
                "vehicles": data_dict["vehicles"][i],
                "starships": data_dict["starships"][i],
                "created": data_dict["created"][i],
                "edited": data_dict["edited"][i],
                "url": data_dict["url"][i]
            }

            rows_to_insert.append(row)

        schema = [
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("height", "INTEGER"),
            bigquery.SchemaField("mass", "INTEGER"),
            bigquery.SchemaField("hair_color", "STRING"),
            bigquery.SchemaField("skin_color", "STRING"),
            bigquery.SchemaField("eye_color", "STRING"),
            bigquery.SchemaField("birth_year", "STRING"),
            bigquery.SchemaField("gender", "STRING"),
            bigquery.SchemaField("homeworld", "STRING"),
            bigquery.SchemaField("films", "STRING"),
            bigquery.SchemaField("species", "STRING"),
            bigquery.SchemaField("vehicles", "STRING"),
            bigquery.SchemaField("starships", "STRING"),
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
            destination_file = os.path.join(directory, "peoples.csv")

          
            if os.path.exists(destination_file):
                print("El archivo 'peoples.csv' ya existe en la carpeta.")
                return

      
            os.rename(source_file, destination_file)

    print("Se han renombrado todos los archivos CSV en la carpeta a 'peoples.csv'.")


