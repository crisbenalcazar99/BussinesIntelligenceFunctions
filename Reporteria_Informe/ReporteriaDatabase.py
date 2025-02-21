import pandas as pd
from sqlalchemy import create_engine


def connect_to_db():
    try:
        engine = create_engine(
            'postgresql://postgres:Security2024**@localhost:5432/CertificadosHistorico'
        )
        return engine
    except Exception as error:
        print(f'Failing coneection to DataBase: {error}')


def cargar_archivo_en_batches(dataframe_excel, tabla_destino, batch_size=2000):
    try:
        engine = connect_to_db()
        if engine is None:
            raise Exception('Failing Connection to Database')

        for i in range(0, len(dataframe_excel), batch_size):
            batch_df = dataframe_excel.iloc[i: i + batch_size]
            # Cargar el lote a la DB
            batch_df.to_sql(tabla_destino, engine, if_exists='append', index=False)
        print(f'Datos insertados en la Tabla{tabla_destino}. Total de Registros: {len(dataframe_excel)}')

    except Exception as e:
        print(f'Error al cargar los datos {e}')
