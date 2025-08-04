from prefect import task, flow
import requests
from time import sleep

# ----------------------
# STEP 1: Extract
# ----------------------
@task(retries=3, retry_delay_seconds=2)
def extract_pokemon_data(pokemon_name: str) -> dict:
    print(f"Fetching data for Pokémon: {pokemon_name}")
    url = f"https://pokeapi.co/api/v2/pokemon/{pokemon_name.lower()}"
    response = requests.get(url)

    if response.status_code != 200:
        print("Failed to fetch data. Retrying...")
        raise Exception("API Error")

    print("Data fetched successfully.")
    return response.json()

# ----------------------
# STEP 2: Transform
# ----------------------
@task
def transform_pokemon_data(data: dict) -> dict:
    print("Transforming data...")
    transformed = {
        "name": data["name"].capitalize(),
        "height": data["height"],
        "weight": data["weight"],
        "types": [t["type"]["name"] for t in data["types"]]
    }
    return transformed

# ----------------------
# STEP 3: Load
# ----------------------
@task
def load_pokemon_data(data: dict):
    print("Loading Pokémon data...")
    print(f"Pokémon Info:\n{data}")

# ----------------------
# Flow
# ----------------------
@flow(name="pokemon-etl-flow")
def pokemon_etl_flow(pokemon_name: str = "pikachu"):
    raw_data = extract_pokemon_data(pokemon_name)
    clean_data = transform_pokemon_data(raw_data)
    load_pokemon_data(clean_data)

# ----------------------
# Run
# ----------------------
if __name__ == "__main__":
    pokemon_etl_flow("arceus")  # Change this to any Pokémon name
