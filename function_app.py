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

    # Notificar inicio del procesamiento
    notify_progress(id, "inprogress", "Starting report generation...", 0)
    update_request(id, "inprogress")
    
    request_info = get_request(id)
    
    sample_size = request_info[0].get("sample_size")
    pokemon_type = request_info[0]["type"]
    
    logger.info(f"Report {id} - Type: {pokemon_type}, Sample Size: {sample_size}")
    
    # Notificar obtención de datos
    notify_progress(id, "inprogress", "Fetching Pokemon data...", 20)
    
    try:
        pokemons = get_pokemons_with_details(pokemon_type, id)
        
        # Aplicar muestreo si es necesario
        if sample_size and isinstance(sample_size, int) and sample_size > 0:
            original_count = len(pokemons)
            sample_size = min(sample_size, original_count)
            if sample_size < original_count:
                notify_progress(id, "inprogress", "Applying random sampling...", 70)
                pokemons = random.sample(pokemons, sample_size)
                logger.info(f"Applied random sampling: {sample_size} of {original_count} records")
            else:
                logger.info(f"Sample size {sample_size} is >= total records {original_count}, using all records")
        else:
            logger.info(f"No sampling applied, using all {len(pokemons)} records")
        
        # Generar CSV
        notify_progress(id, "inprogress", "Generating CSV file...", 80)
        pokemon_bytes = generate_csv_to_blob(pokemons)
        
        # Subir a blob
        notify_progress(id, "inprogress", "Uploading to storage...", 90)
        blob_name = f"poke_report_{id}.csv"
        upload_csv_to_blob(blob_name=blob_name, csv_data=pokemon_bytes)
        
        # Finalizar
        url_completa = f"https://{STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{BLOB_CONTAINER_NAME}/{blob_name}"
        notify_progress(id, "completed", f"Report completed! {len(pokemons)} Pokemon processed.", 100)
        update_request(id, "completed", url_completa)
        
    except Exception as e:
        logger.error(f"Error processing report {id}: {e}")
        notify_progress(id, "failed", f"Error: {str(e)}", 0)
        update_request(id, "failed")


def notify_progress(report_id: int, status: str, message: str, progress: int):
    """Notificar progreso a través del SSE endpoint"""
    try:
        payload = {
            "type": "progress_update",
            "data": {
                "reportId": report_id,
                "status": status,
                "message": message,
                "progress": progress,
                "timestamp": datetime.datetime.now().isoformat()
            }
        }
        
        response = requests.post(f"{DOMAIN}/notify-progress", json=payload, timeout=5)
        if response.status_code == 200:
            logger.info(f"Progress notification sent for report {report_id}: {progress}%")
        else:
            logger.warning(f"Failed to send progress notification: {response.status_code}")
            
    except Exception as e:
        logger.error(f"Error sending progress notification: {e}")


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

def get_pokemons_with_details(type: str, report_id: int) -> list:
    pokeapi_url = f"https://pokeapi.co/api/v2/type/{type}"
    response = requests.get(pokeapi_url, timeout=30)
    data = response.json()
    pokemon_entries = data.get("pokemon", [])
    
    total_pokemon = len(pokemon_entries)
    detailed_pokemons = []
    
    for i, entry in enumerate(pokemon_entries):
        pokemon = entry["pokemon"]
        try:
            details = get_pokemon_details(pokemon["url"])
            detailed_pokemons.append({
                "name": pokemon["name"],
                "url": pokemon["url"],
                **details
            })
            
            # Notificar progreso cada 10 Pokemon procesados o en el último
            if (i + 1) % 10 == 0 or (i + 1) == total_pokemon:
                progress = 20 + int((i + 1) / total_pokemon * 50)  # 20-70%
                notify_progress(
                    report_id, 
                    "inprogress", 
                    f"Processing Pokemon details... ({i + 1}/{total_pokemon})", 
                    progress
                )
                
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
    response = requests.get(pokemon_url, timeout=10)
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