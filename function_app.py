import random
import io
import os
import azure.functions as func
import datetime
import json
import logging
import requests
from dotenv import load_dotenv
import pandas as pd
from azure.storage.blob import BlobServiceClient

app = func.FunctionApp()
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DOMAIN = os.getenv("DOMAIN")
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
BLOB_CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME")
STORAGE_ACCOUNT_NAME = os.getenv("STORAGE_ACCOUNT_NAME")

@app.queue_trigger(
    arg_name="azqueue",
    queue_name="requests",
    connection="QueueAzureWebJobsStorage"
)
def QueueTriggerPokeReport(azqueue: func.QueueMessage):
    body = azqueue.get_body().decode('utf-8')
    record = json.loads(body)
    id = record[0]["id"]
    
    logger.info(f"Processing report {id}")

    update_request(id, "inprogress")
    request_info = get_request(id)
    
    sample_size = request_info[0].get("sample_size")
    pokemon_type = request_info[0]["type"]
    
    logger.info(f"Report {id} - Type: {pokemon_type}, Sample Size: {sample_size}")
    
    pokemons = get_pokemons_with_details(pokemon_type)
    
    if sample_size and isinstance(sample_size, int) and sample_size > 0:
        original_count = len(pokemons)
        sample_size = min(sample_size, original_count)  # Ensure we don't exceed available pokemons
        if sample_size < original_count:
            pokemons = random.sample(pokemons, sample_size)
            logger.info(f"Applied random sampling: {sample_size} of {original_count} records")
        else:
            logger.info(f"Sample size {sample_size} is >= total records {original_count}, using all records")
    else:
        logger.info(f"No sampling applied, using all {len(pokemons)} records")
    
    pokemon_bytes = generate_csv_to_blob(pokemons)
    blob_name = f"poke_report_{id}.csv"
    upload_csv_to_blob(blob_name=blob_name, csv_data=pokemon_bytes)
    
    url_completa = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{BLOB_CONTAINER_NAME}/{blob_name}"
    update_request(id, "completed", url_completa)


def update_request(id: int, status: str, url: str = None) -> dict:
    payload = {
        "status": status,
        "id": id
    }
    if url:
        payload["url"] = url

    response = requests.put(f"{DOMAIN}/request", json=payload)
    return response.json()

def get_request(id: int) -> dict:
    response = requests.get(f"{DOMAIN}/request/{id}")
    return response.json()

def get_pokemons_with_details(type: str) -> list:
    pokeapi_url = f"https://pokeapi.co/api/v2/type/{type}"
    response = requests.get(pokeapi_url, timeout=3000)
    data = response.json()
    pokemon_entries = data.get("pokemon", [])
    
    detailed_pokemons = []
    for entry in pokemon_entries:
        pokemon = entry["pokemon"]
        try:
            details = get_pokemon_details(pokemon["url"])
            detailed_pokemons.append({
                "name": pokemon["name"],
                "url": pokemon["url"],
                **details
            })
        except Exception as e:
            logger.error(f"Error obteniendo detalles para {pokemon['name']}: {e}")
            # Añadir el Pokémon básico si falla la obtención de detalles
            detailed_pokemons.append({
                "name": pokemon["name"],
                "url": pokemon["url"],
                "hp": "N/A",
                "attack": "N/A",
                "defense": "N/A",
                "special_attack": "N/A",
                "special_defense": "N/A",
                "speed": "N/A",
                "abilities": "N/A",
                "height": "N/A",
                "weight": "N/A"
            })
    
    return detailed_pokemons

def get_pokemon_details(pokemon_url: str) -> dict:
    response = requests.get(pokemon_url, timeout=3000)
    data = response.json()
    
    # Extraer estadísticas base
    stats = {stat["stat"]["name"]: stat["base_stat"] for stat in data["stats"]}
    
    # Procesar habilidades (tomamos las 3 primeras)
    abilities = ", ".join([ability["ability"]["name"] for ability in data["abilities"][:3]])
    
    return {
        "hp": stats.get("hp", 0),
        "attack": stats.get("attack", 0),
        "defense": stats.get("defense", 0),
        "special_attack": stats.get("special-attack", 0),
        "special_defense": stats.get("special-defense", 0),
        "speed": stats.get("speed", 0),
        "abilities": abilities,
        "height": data.get("height", 0) / 10,  # Convertir a metros
        "weight": data.get("weight", 0) / 10   # Convertir a kg
    }

def generate_csv_to_blob(pokemon_list: list) -> bytes:
    df = pd.DataFrame(pokemon_list)
    
    # Reordenar columnas para mejor presentación
    columns_order = [
        'name', 'hp', 'attack', 'defense', 'special_attack', 
        'special_defense', 'speed', 'height', 'weight', 
        'abilities', 'url'
    ]
    df = df[columns_order]
    
    # Renombrar columnas para mejor legibilidad
    df.columns = [
        'Name', 'HP', 'Attack', 'Defense', 'Special Attack',
        'Special Defense', 'Speed', 'Height (m)', 'Weight (kg)',
        'Abilities', 'URL'
    ]
    
    output = io.StringIO()
    df.to_csv(output, index=False, encoding='utf-8')
    csv_bytes = output.getvalue().encode('utf-8')
    output.close()
    return csv_bytes

def upload_csv_to_blob(blob_name: str, csv_data: bytes):
    try:
        blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=BLOB_CONTAINER_NAME, blob=blob_name)
        blob_client.upload_blob(csv_data, overwrite=True)
    except Exception as e:
        logger.error(f"Error al subir el archivo {e}")
        raise