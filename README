# Workshop 02 By Jose Carrera
# ETL Process Using Apache Airflow

## Overview
This project focuses on extracting, transforming, and loading (ETL) data using Apache Airflow. It specifically deals with data from Spotify and the Grammy Awards, which are stored in a MySQL database, cleaned, and merged for analysis. Finally, the data is visualized using Looker.
### Tools used

- ![Python](https://img.shields.io/badge/python-%2314354C.svg?style=for-the-badge&logo=python&logoColor=white) Python
- ![Jupyter](https://img.shields.io/badge/jupyter-%23F37626.svg?style=for-the-badge&logo=jupyter&logoColor=white) Jupyter Notebooks
- ![MySQL ](https://img.shields.io/badge/MySQL-%2300f.svg?style=for-the-badge&logo=mysql&logoColor=white) MYSQL
- ![Power BI](https://img.shields.io/badge/powerbi-F2C811.svg?style=for-the-badge&logo=powerbi&logoColor=black) Power BI
- ![Airflow](https://img.shields.io/badge/airflow-F2C811.svg?style=for-the-badge&logo=powerbi&logoColor=black) Airflow

## Prerequisites
1. Install Python: [Python Downloads](https://www.python.org/downloads/)
2. Install WSL: [WSL Downloads](https://learn.microsoft.com/en-us/windows/wsl/install)
3. Install Power BI: [Install Power BI Desktop](https://powerbi.microsoft.com/en-us/desktop/)

## Required Python Libraries
- pandas
- mysql-connector-python
- logging
- SQLALCHEMY
- seaborn
- numpy
- matplot

You can install these libraries using pip:

pip install pandas mysql-connector-python SQLAlchemy matplotlib seaborn



## Setup Instructions

WSL-UBUNTU
1. *Clone the project:*

    bash
    ```
    git clone https://github.com/JOSEDANCAR/WORKSHOP-2
    ```
    

2. *Go to the project directory:*

    ```
    cd WorkShop2_ETL
    ```

3. *Create a virtual environment for Python:*

    ```
    python -m venv venv
    ```
4. *Activate the virtual environment:*

   - On macOS/Linux:
    
    ```
     source venv/bin/activate
     ```

5. *Install the required libraries:*

    ```
    pip install -r requirements.txt
    ```

6. *Set Up the Database:*

    -  use the command line to connect to your MySQL server.
    - Create the database by running the following SQL command:

      
    ```
    sql
      CREATE DATABASE spotify_grammy_db;
      ```

7. *Explore the Project:*

  - Start with the folder *Data* :

    - You can use a CSV file and load it using MySQL or through a script if the data is available in a file format.
    - Dataset to be readed as CSV :Spotify dataset [Downloads](https://learn.microsoft.com/en-us/windows/wsl/install)
    -  Dataset to be loaded into the initial database: 
    Grammys Dataset- Spotify dataset [Downloads](https://www.kaggle.com/datasets/unanimad/grammy-awards)
 - Next open the folder *db_operations*:
      - Execute - *001_code.ipynb*:

        ## Database Connection:

        The notebook establishes a connection to a MySQL database named 'spotify_grammy_db' using the root user.


        ## Data Insertion:

        Two main datasets are being inserted into the database:
        a. Spotify dataset: Inserted into the 'spotify_dataset' table.
        b. Grammy Awards dataset: Inserted into the 'the_grammy_awards' table.


        ## Data Verification:

        After insertion, the notebook queries and displays the first 10 rows of each table to verify the data.


        ## Key Libraries Used:

        pandas: For data manipulation
        mysql.connector: For MySQL database connection
        sqlalchemy: For database operations using DataFrame.to_sql() method


        ## Data Cleaning:

        NaN values are replaced with default values before insertion.
        The 'Unnamed: 0' column is dropped if present in the CSV files.


        ## Error Handling:

        The code includes try-except blocks to handle potential errors during data insertion.


9. Continue with the folder *EDA* and run : 

    - ## EDA: In this notebooks, we will dive into a comprehensive exploration of the dataset.

        ## 001_EDA_spotify_dataset.ipynb

        Exploratory Data Analysis (EDA) for the spotify_dataset Data

        This notebook performs an exploratory data analysis on the spotify_dataset table extracted from a MySQL database. We will explore the data distribution, identify key patterns, and visualize important relationships within the data.

        ## 002_EDA_the_grammy_awards.ipynb
   

        Exploratory Data Analysis (EDA) for the the_grammy_awards Data

        This notebook performs an exploratory data analysis on the `the_grammy_awards` table extracted from a MySQL database. We will explore the data distribution, identify key patterns, and visualize important relationships within the data.



10. To execute *DAG :*
- Go to the proyect command `airflow dags trigger spotify_grammy_outer_join`
- Run the following commmand `airflow webServer`
- Run the following command in the terminal:  `airflow scheduler` to start the scheduler.
- Open a web browser and navigate to `http://localhost:8080` to access the Air
flow UI.
- In the Airflow UI, navigate to the "DAGs" tab and select the "
spotify_grammy_outer_join" DAG.

    ## DAG.PY

    ## Descripción
    Este archivo `dag.py` define un DAG (Directed Acyclic Graph) en Apache Airflow que se encarga de integrar datos de dos fuentes: un conjunto de datos de Spotify y los premios Grammy. Utiliza MySQL como base de datos para almacenar los datos fusionados.

    ## Funciones

    ### `conectar_mysql()`
    - **Descripción**: Establece una conexión con la base de datos MySQL `spotify_grammy_db`.
    - **Retorno**: Devuelve el objeto de conexión si es exitosa; en caso de error, registra el error y lanza una excepción.

    ### `clean_and_outer_join()`
    - **Descripción**: 
    - Carga los datos desde las tablas `spotify_dataset` y `the_grammy_awards`.
    - Realiza un `outer join` entre los dos DataFrames utilizando `track_name` y `title` como claves de unión.
    - Limpia los datos reemplazando valores NaN con valores por defecto y asegura que los tipos de datos sean correctos.
    - Crea o limpia la tabla `Spotify_Grrammy_Merge` en MySQL y luego inserta los datos fusionados.
    - **Registro**: Registra información sobre el proceso, incluyendo el número de filas cargadas, el resultado del merge y el estado de las inserciones.

    ## Uso
    Para ejecutar este script, asegúrate de tener Apache Airflow y MySQL configurados correctamente. Al ejecutar el DAG, se realizará la integración de datos según lo descrito.

    ## Requisitos
    - Python 3.x
    - Bibliotecas: `airflow`, `mysql-connector-python`, `pandas`
    - Acceso a una base de datos MySQL con las tablas requeridas.

    ## Ejecución
    Este DAG se puede ejecutar desde la interfaz de usuario de Apache Airflow o mediante la línea de comandos. Asegúrate de que el entorno de ejecución tenga acceso a la base de datos y a los 

    ## Conexión de la base de datos `spotify_grammy_db` a Power BI



## Visualizacion del dashboard

### Paso 1: Conectar a la base de datos MySQL

1. Abre **Power BI Desktop** en tu computadora.
2. En la barra superior, selecciona la opción **Get Data** (Obtener Datos).
3. En la ventana que aparece, busca y selecciona **MySQL database**. Si no aparece en la lista inicial, haz clic en **More...** (Más...) y utiliza el buscador para encontrar la opción de **MySQL**.
4. Haz clic en **Connect** (Conectar) para iniciar el proceso de conexión.

### Paso 2: Ingresar la información de conexión

1. En la ventana emergente, proporciona la siguiente información:
   - **Server**: Especifica la dirección del servidor MySQL. Si la base de datos está en tu máquina local, usa `localhost`. 
   - **Database**: Ingresa el nombre de la base de datos, en este caso, `spotify_grammy_db`.

   Trabajando en el entorno de Windows con WSL, puedes verificar la IP de MySQL ejecutando este comando en WSL:
   

   `hostname`

   ### Paso 3: Ingresar las credenciales de usuario

1. En la siguiente pantalla, introduce tus credenciales de MySQL:
   - **Username**: El nombre de usuario de la base de datos MySQL `root`.
   - **Password**: La contraseña correspondiente, en este caso `root`.

2. Haz clic en **Connect** para continuar.

### Paso 4: Seleccionar y cargar las tablas

1. Después de conectarte correctamente a la base de datos, Power BI mostrará una lista de las tablas disponibles.
2. Selecciona las tablas necesarias para tu análisis, como:
   - `Spotify`
   - `Grammy_awards`
   
3. Haz clic en **Load** (Cargar) para cargar los datos de las tablas seleccionadas en Power BI.

  


