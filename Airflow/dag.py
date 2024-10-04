from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import mysql.connector
import pandas as pd
from datetime import datetime
import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def conectar_mysql():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='root',
            database='spotify_grammy_db'
        )
        logger.info("Conexión a MySQL exitosa")
        return connection
    except mysql.connector.Error as err:
        logger.error(f"Error al conectar a MySQL: {err}")
        raise

def clean_and_outer_join():
    logger.info("Iniciando la función clean_and_outer_join")
    connection = conectar_mysql()
    
    try:
        # Cargar los datos desde MySQL
        spotify_df = pd.read_sql("SELECT * FROM spotify_dataset", connection)
        grammy_df = pd.read_sql("SELECT * FROM the_grammy_awards", connection)
        
        logger.info(f"Datos cargados - Spotify: {len(spotify_df)} filas, Grammy: {len(grammy_df)} filas")

        # Realizar el outer join
        merged_df = pd.merge(spotify_df, grammy_df, how='outer', left_on='track_name', right_on='title')
        logger.info(f"Merge completado - Resultado: {len(merged_df)} filas")

        # Reemplazar NaN con valores por defecto
        merged_df = merged_df.fillna({
            'track_id': '',
            'artists': '',
            'album_name': '',
            'track_name': '',
            'popularity': 0,
            'duration_ms': 0,
            'explicit': False,
            'danceability': 0.0,
            'energy': 0.0,
            'key': 0,
            'loudness': 0.0,
            'mode': 0,
            'speechiness': 0.0,
            'acousticness': 0.0,
            'instrumentalness': 0.0,
            'liveness': 0.0,
            'valence': 0.0,
            'tempo': 0.0,
            'time_signature': 0,
            'track_genre': 'Unknown',
            'year': 0,
            'title': '',
            'published_at': datetime(2000, 1, 1),
            'updated_at': datetime(2000, 1, 1),
            'category': '',
            'nominee': '',
            'artist': '',
            'workers': '',
            'img': '',
            'winner': False
        })

        # Asegurar tipos de datos correctos
        int_columns = ['popularity', 'duration_ms', 'key', 'mode', 'time_signature', 'year']
        float_columns = ['danceability', 'energy', 'loudness', 'speechiness', 'acousticness', 
                         'instrumentalness', 'liveness', 'valence', 'tempo']
        bool_columns = ['explicit', 'winner']
        
        for col in int_columns:
            if col in merged_df.columns:
                merged_df[col] = merged_df[col].astype(int)
        for col in float_columns:
            if col in merged_df.columns:
                merged_df[col] = merged_df[col].astype(float)
        for col in bool_columns:
            if col in merged_df.columns:
                merged_df[col] = merged_df[col].astype(bool)

        # Crear cursor y limpiar tabla existente
        cursor = connection.cursor()
        
        # Crear la tabla si no existe
        create_table_query = """
        CREATE TABLE IF NOT EXISTS Spotify_Grrammy_Merge (
            track_id VARCHAR(255),
            artists TEXT,
            album_name VARCHAR(255),
            track_name TEXT,
            popularity INT,
            duration_ms INT,
            explicit BOOLEAN,
            danceability FLOAT,
            energy FLOAT,
            `key` INT,
            loudness FLOAT,
            mode INT,
            speechiness FLOAT,
            acousticness FLOAT,
            instrumentalness FLOAT,
            liveness FLOAT,
            valence FLOAT,
            tempo FLOAT,
            time_signature INT,
            track_genre VARCHAR(255),
            year INT,
            title VARCHAR(255),
            published_at DATETIME,
            updated_at DATETIME,
            category VARCHAR(255),
            nominee VARCHAR(255),
            artist VARCHAR(255),
            workers TEXT,
            img TEXT,
            winner BOOLEAN
        )
        """
        cursor.execute(create_table_query)
        logger.info("Tabla Spotify_Grrammy_Merge creada o ya existente")

        # Limpiar la tabla antes de insertar nuevos datos
        cursor.execute("TRUNCATE TABLE Spotify_Grrammy_Merge")
        logger.info("Tabla Spotify_Grrammy_Merge limpiada")

        # Preparar la consulta de inserción
        insert_query = """
        INSERT INTO Spotify_Grrammy_Merge (
            track_id, artists, album_name, track_name, popularity, duration_ms, explicit,
            danceability, energy, `key`, loudness, mode, speechiness, acousticness,
            instrumentalness, liveness, valence, tempo, time_signature, track_genre,
            year, title, published_at, updated_at, category, nominee, artist, workers, img, winner
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Convertir las filas del DataFrame en tuplas y evitar valores inconsistentes
        insert_data = merged_df.astype(object).where(pd.notnull(merged_df), None).values.tolist()
        
        # Insertar los datos por lotes
        batch_size = 1000
        for i in range(0, len(insert_data), batch_size):
            batch = insert_data[i:i + batch_size]
            cursor.executemany(insert_query, batch)
            connection.commit()
            logger.info(f"Insertado lote de {len(batch)} filas")

        logger.info(f"Total de filas insertadas en Spotify_Grrammy_Merge: {len(insert_data)}")

    except Exception as e:
        logger.error(f"Error durante la ejecución: {str(e)}")
        connection.rollback()
        raise
    finally:
        cursor.close()
        connection.close()
        logger.info("Conexión cerrada")

# Definir el DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG('spotify_grammy_outer_join', 
         default_args=default_args,
         description='A DAG to merge Spotify and Grammy data',
         schedule_interval=None,
         catchup=False) as dag:

    clean_and_join_task = PythonOperator(
        task_id='clean_and_outer_join',
        python_callable=clean_and_outer_join,
    )

clean_and_join_task