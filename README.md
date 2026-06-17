# Guía de Instalación y Configuración de Apache Airflow 3 en WSL

Esta guía detalla el proceso paso a paso para realizar una instalación limpia de **Apache Airflow 3** de forma nativa en Windows utilizando **WSL (Windows Subsystem for Linux)**, evitando el uso de Docker. También incluye la solución a problemas comunes de permisos, rutas de archivos y conexión de base de datos encontrados durante el proceso.

---

## 📋 Requisitos Previos
* Windows 10 o 11.
* WSL instalado (Distribución Ubuntu recomendada).
* VS Code instalado en Windows con la extensión **WSL** (de Microsoft).

---

## 🚀 Paso 1: Configurar el Entorno en la Ruta Nativa de Linux
> ⚠️ **Nota crítica de rendimiento:** No uses la ruta montada de tu disco de Windows (`/mnt/c/...`). Ejecutar Airflow y su base de datos SQLite allí causa lentitud extrema, problemas de rendimiento y bloqueos de archivos. Utiliza siempre el directorio nativo de Linux (`/home/usuario/...`).

1. Abre tu terminal de Ubuntu en WSL y crea una carpeta exclusiva para tu proyecto:
   ```bash
   mkdir ~/airflow_workspace
   cd ~/airflow_workspace
   ```
2. Abre este directorio en VS Code para trabajar de forma nativa en Linux:
   ```bash
   code .
   ```

---

## 📦 Paso 2: Crear el Entorno Virtual e Instalar Airflow
Dentro de la terminal integrada de VS Code (que ahora corre en WSL):

1. Crea y activa tu entorno virtual de Python:
   ```bash
   python3 -m venv airflow_env
   source airflow_env/bin/activate
   ```
   *(Verás el indicador `(airflow_env)` al inicio de tu prompt).*

2. Configura la variable de entorno para definir dónde se guardarán los archivos de Airflow:
   ```bash
   export AIRFLOW_HOME=~/airflow_workspace/airflow
   ```

3. Instala Airflow 3.2.0 utilizando las restricciones oficiales para garantizar la compatibilidad de dependencias (ejemplo para Python 3.10):
   ```bash
   pip install "apache-airflow[celery]==3.2.0" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.2.0/constraints-3.10.txt"
   ```

---

## ⚙️ Paso 3: Inicializar la Base de Datos y Desactivar Ejemplos
1. Genera los archivos de configuración y la base de datos inicial:
   ```bash
   airflow db migrate
   ```
   *(Nota: En Airflow 3.x, el comando `airflow db init` está obsoleto y se ha reemplazado por `migrate`).*

2. Desactiva la carga de DAGs de ejemplo para mantener limpia la interfaz:
   ```bash
   sed -i 's/load_examples = True/load_examples = False/g' ~/airflow_workspace/airflow/airflow.cfg
   ```

3. Aplica un reinicio a la base de datos para limpiar cualquier registro previo de ejemplos:
   ```bash
   airflow db reset -y
   airflow db migrate
   ```

---

## ✍️ Paso 4: Crear tu Primer DAG
1. Crea la estructura de carpetas necesaria para tus DAGs dentro de la ruta de Airflow:
   ```bash
   mkdir -p ~/airflow_workspace/airflow/dags
   touch ~/airflow_workspace/airflow/dags/first_dag.py
   ```

2. Abre `first_dag.py` en tu editor y pega el siguiente código de prueba, el cual utiliza `PythonOperator` e intercambia datos mediante XComs:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    'owner': 'coder2j',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def greet(ti):
    first_name = ti.xcom_pull(task_ids='get_name', key='first_name')
    last_name = ti.xcom_pull(task_ids='get_name', key='last_name')
    age = ti.xcom_pull(task_ids='get_age', key='age')
    print(f"Hello, World! My name is {first_name} {last_name} and I am {age} years old.")

def get_name(ti):
    first_name = 'John'
    last_name = 'Doe'
    ti.xcom_push(key='first_name', value=first_name)
    ti.xcom_push(key='last_name', value=last_name)

def get_age(ti):
    ti.xcom_push(key='age', value=20)    

with DAG(
    default_args=default_args,
    dag_id='our_first_dag',
    description='Our first DAG',
    start_date=datetime(2026, 6, 13),
    schedule='@daily',
) as dag:
    task1 = PythonOperator(
        task_id='greet',
        python_callable=greet
    )

    task2 = PythonOperator(
        task_id='get_name',
        python_callable=get_name
    )
    
    task3 = PythonOperator(
        task_id='get_age',
        python_callable=get_age
    )

[task2, task3] >> task1
```

---

## 🏃 Paso 5: Ejecución
Para arrancar todos los servicios de Airflow a la vez en modo desarrollo, ejecuta:
```bash
airflow standalone
```
* Busca en la salida de la terminal las credenciales autogeneradas (`admin` y su contraseña temporal).
* Abre tu navegador en Windows e ingresa a `http://localhost:8080`.
* Inicia sesión, activa el interruptor de `our_first_dag` y haz clic en el botón **Trigger** para ejecutarlo.

---

## 🛠️ Solución de Problemas (Troubleshooting)

### 1. Olvidé o perdí la contraseña de mi usuario de WSL
Si necesitas ejecutar comandos administrativos con `sudo` pero no recuerdas tu clave de Linux, puedes restablecerla desde la terminal de Windows:
1. Abre **PowerShell** en Windows y accede directamente como `root` (sin contraseña):
   ```powershell
   wsl -u root
   ```
2. Cambia la contraseña de tu usuario de Linux indicando su nombre de usuario:
   ```bash
   passwd tu_nombre_de_usuario
   ```
3. Escribe `exit` para cerrar la sesión de root.

### 2. Error `sqlalchemy.exc.TimeoutError: QueuePool limit reached`
Este error se presenta debido a que el servidor de la API de Airflow 3.x (`api-server`) maneja solicitudes de forma asíncrona, lo que puede agotar el límite de conexiones por defecto de SQLAlchemy.
* **Solución:** Abre tu archivo `airflow/airflow.cfg`, localiza la sección `[database]` e incrementa los límites de conexión de la siguiente manera:
  ```ini
  [database]
  sql_alchemy_pool_size = 15
  sql_alchemy_max_overflow = 30
  ```

### 3. Advertencia de Deprecación de `PythonOperator`
Si visualizas una advertencia indicando que `airflow.operators.python.PythonOperator` está obsoleto:
* **Solución:** Modifica la importación en tus archivos DAG. En Airflow 3.x, los operadores estándar se han movido a un proveedor dedicado:
  * `from airflow.providers.standard.operators.python import PythonOperator`
