import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, select
import logging
from datetime import datetime
import json

# Configuración del logging
logging.basicConfig(filename='excel_to_sql.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

def load_config(config_path):
    """Carga la configuración desde un archivo JSON."""
    with open(config_path, 'r') as file:
        config = json.load(file)
    return config

def excel_to_sql(config):
    """Convierte una hoja de Excel en una tabla SQL."""
    # Cargar la hoja de Excel
    df = pd.read_excel(config['excel_path'], sheet_name=config['sheet_name'])

    # Crear conexión a la base de datos
    engine = create_engine(config['db_url'])
    metadata = MetaData()
    connection = engine.connect()

    # Verificar si la tabla ya existe
    if engine.dialect.has_table(connection, config['table_name']):
        logging.info(f"La tabla {config['table_name']} ya existe. Actualizando registros...")
        existing_table = Table(config['table_name'], metadata, autoload_with=engine)
        existing_data = pd.read_sql_table(config['table_name'], engine)

        # Identificar nuevos registros y actualizaciones
        new_records = df[~df.isin(existing_data.to_dict('list')).all(1)]
        updated_records = df[df.isin(existing_data.to_dict('list')).all(1)]

        # Insertar nuevos registros
        if not new_records.empty:
            new_records.to_sql(config['table_name'], engine, if_exists='append', index=False)
            logging.info(f"Insertados {len(new_records)} nuevos registros.")

        # Actualizar registros existentes
        if not updated_records.empty:
            for index, row in updated_records.iterrows():
                update_stmt = existing_table.update().where(
                    existing_table.c[config['primary_key']] == row[config['primary_key']]
                ).values(row.to_dict())
                connection.execute(update_stmt)
            logging.info(f"Actualizados {len(updated_records)} registros.")
    else:
        # Crear nueva tabla si no existe
        df.to_sql(config['table_name'], engine, index=False)
        logging.info(f"Tabla {config['table_name']} creada con {len(df)} registros.")

    connection.close()

# Cargar configuración
config_path = 'config.json'
config = load_config(config_path)

# Ejecutar la función principal
excel_to_sql(config)
