from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
import json
import os
from html import unescape

def json_pets_to_db():
    json_path = "/opt/airflow/data/pets-data.json"
    
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    flattened_records = []
    for pet in data['pets']:
        fav_foods = pet.get('favFoods', [None])
        
        if fav_foods == [None]:
            flattened_records.append({
                'name': pet['name'],
                'species': pet['species'],
                'birth_year': pet['birthYear'],
                'photo_url': pet['photo'].strip(),
                'fav_food': None
            })
        else:
            for food in fav_foods:
                clean_food = unescape(
                    food.replace('<strong>', '').replace('</strong>', '').strip()
                ) if food else None
                
                flattened_records.append({
                    'name': pet['name'],
                    'species': pet['species'],
                    'birth_year': pet['birthYear'],
                    'photo_url': pet['photo'].strip(),
                    'fav_food': clean_food
                })
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_pets')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pets (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            species VARCHAR(50) NOT NULL,
            birth_year INTEGER,
            photo_url TEXT,
            fav_food VARCHAR(255)
        )
    """)
    
    cursor.execute("TRUNCATE TABLE pets RESTART IDENTITY")
    
    insert_query = """
        INSERT INTO pets (name, species, birth_year, photo_url, fav_food)
        VALUES (%s, %s, %s, %s, %s)
    """
    for record in flattened_records:
        cursor.execute(insert_query, (
            record['name'],
            record['species'],
            record['birth_year'],
            record['photo_url'],
            record['fav_food']
        ))
    
    conn.commit()
    cursor.close()
    conn.close()

with DAG(
    dag_id='dag_pets_json',
    start_date=datetime(2026, 2, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    flatten_task = PythonOperator(
        task_id='dag_pets_json_task',
        python_callable=json_pets_to_db
    )