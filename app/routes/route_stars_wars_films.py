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
    prefix="/stars-wars/films",
    tags=["stars-wars-films-MS"]
)



@route.get("/getFilms")
async def getFilms():
    APIFILMS = os.getenv("APIFILMS");
    URI = str(APIFILMS);
    films = requests.get(URI);
    data = films.json();
    resultsFilms = data["results"];
    dataFilms = resultsFilms;
    
    contador=0;
    arrayFilms = [];
    while contador < len(dataFilms):
          objeto = dataFilms[contador];
          arrayFilms.append(objeto);
          print(arrayFilms);
          contador +=1;

    df = pd.DataFrame(arrayFilms);
    csv_data = df.to_csv(index=False, sep=";");

    response = Response(content=csv_data, media_type="text/csv", headers={"Content-Disposition": "attachment; filename=films.csv"})

    return response     
   
@route.post("/subir-csv-gcp/films")
async def subir_csv_gcp_films():
    directory = 'C:\\Users\\Ignacio\\Downloads' 
    rename_csv_files(directory)  
    download_carpeta = 'C:\\Users\\Ignacio\\Downloads'
    archivos_en_descargas = os.listdir(download_carpeta)
    archivo_deseado = "films.csv" if "films.csv" in archivos_en_descargas else None

    
    if archivo_deseado in archivos_en_descargas:
        ruta_de_archivo = os.path.join(download_carpeta, archivo_deseado)
        df = pd.read_csv(ruta_de_archivo, sep=";")
        
        df['edited'] = df['edited'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ").isoformat())
        df['edited'] = df['edited'].str.replace('\.\d{3}Z', '', regex=True)

        df['created'] = df['created'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%S.%fZ").isoformat())
        df['created'] = df['created'].str.replace('\.\d{3}Z', '', regex=True)

        titles = df["title"].tolist();
        episode_ids = df["episode_id"].tolist();
        opening_crawls = df["opening_crawl"].tolist();
        directors = df["director"].tolist();
        producers = df["producer"].tolist();
        release_dates =df["release_date"].tolist();
        characters = df["characters"].tolist();
        planets = df["planets"].tolist();
        starships = df["starships"].tolist();
        vehicles = df["vehicles"].tolist();
        species =df["species"].tolist();
        createds = df["created"].tolist();
        editeds = df["edited"].tolist();
        urls = df["url"].tolist();

        diccionario_de_datos = {
            "title":titles,
            "episode_id":episode_ids,
            "opening_crawl":opening_crawls,
            "director":directors,
            "producer":producers,
            "release_date":release_dates,
            "characters":characters,
            "planets":planets,
            "starships":starships,
            "vehicles":vehicles,
            "species":species,
            "created":createds,
            "edited":editeds,
            "url": urls
        }

        cliente = bigquery.Client();
        dataset_id = "stars_wars"
        table_id = "stars_wars_films"
        table_ref = cliente.dataset(dataset_id).table(table_id);

        insertar_filas = [];
        for i in range(len(diccionario_de_datos["title"])):
            row = {
                "title":diccionario_de_datos["title"][i],
                "episode_id":diccionario_de_datos["episode_id"][i],
                "opening_crawl":diccionario_de_datos["opening_crawl"][i],
                "director":diccionario_de_datos["director"][i],
                "producer":diccionario_de_datos["producer"][i],
                "release_date":diccionario_de_datos["release_date"][i],
                "characters":diccionario_de_datos["characters"][i],
                "planets":diccionario_de_datos["planets"][i],
                "starships":diccionario_de_datos["starships"][i],
                "vehicles":diccionario_de_datos["vehicles"][i],
                "species":diccionario_de_datos["species"][i],
                "created":createds[i],
                "edited":editeds[i],
                "url": urls[i]
            }

            insertar_filas.append(row)
        
        schema =[
            bigquery.SchemaField("title","STRING"),
            bigquery.SchemaField("episode_id","INTEGER"),
            bigquery.SchemaField("opening_crawl","STRING"),
            bigquery.SchemaField("director","STRING"),
            bigquery.SchemaField("producer","STRING"),
            bigquery.SchemaField("release_date","DATE"),
            bigquery.SchemaField("characters","STRING"),
            bigquery.SchemaField("planets","STRING"),
            bigquery.SchemaField("starships","STRING"),
            bigquery.SchemaField("vehicles","STRING"),
            bigquery.SchemaField("species","STRING"),
            bigquery.SchemaField("created", "DATETIME"),
            bigquery.SchemaField("edited", "DATETIME"),
            bigquery.SchemaField("url", "STRING")
        ]

        errors = cliente.insert_rows(table_ref,insertar_filas,selected_fields=schema);

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
            destination_file = os.path.join(directory, "films.csv")

            
            if os.path.exists(destination_file):
                print("El archivo 'films.csv' ya existe en la carpeta.")
                return


            os.rename(source_file, destination_file)

    print("Se han renombrado todos los archivos CSV en la carpeta a 'films.csv'.")
