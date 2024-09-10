from datetime import timedelta, datetime

import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd

import time


class ConcatenatedDataFrames(BaseEstimator, TransformerMixin):
    """
    Concatenate two dataframes.
    :param X: Dataframe to be used to concatenate.
    :param df: Dataframe to be concatenated.
    :param axis: Axis to be concatenated.
    :return: Dataframe concatenated.
    """

    def __init__(self, df1, df2, axis=1):
        self.df1 = df1
        self.df2 = df2
        self.axis = axis

    def fit(self, X, y=None):
        return self

    def transform(self, X=None):
        start_time = time.time()
        print('Dataframe 1')
        self.df1.info()
        print('Dataframe 2')
        self.df2.info()
        print('Proceso de concatenacion')
        self.df1.reset_index(drop=True, inplace=True)
        self.df2.reset_index(drop=True, inplace=True)
        print(set(self.df1.columns).intersection(set(self.df2.columns)))

        X = pd.concat([self.df1, self.df2], axis=self.axis, ignore_index=True)
        X.reset_index(drop=True, inplace=True)
        print(f"Tiempo de ejecucion ConcatenatedDataFrames {time.time() - start_time}")
        X.info()
        return X


class DeterminateExpiredSignatures(BaseEstimator, TransformerMixin):
    """
    Determine the signatures that have expired.
    :param X: Dataframe to be used to determine the signatures.
    :param column: Column to be used to determine the signatures.
    :return: Dataframe with the signatures determined.
    """

    def __init__(self, column_date, column_state_signature='estado_firma'):
        self.column_date = column_date
        self.column_state_signature = column_state_signature

    def fit(self, X, y=None):
        return self

    def transform(self, X=None):
        start_time = time.time()
        # Convertir la fecha actual al formato de pandas
        today = pd.to_datetime('today').normalize()

        # Aplicar la condición y actualizar la columna 'column_estado_firma'
        X.loc[X[self.column_date] < today, self.column_state_signature] = 'Caducado'
        print(f"Tiempo de ejecucion DeterminarFirmasCaducadas {time.time() - start_time}")
        return X


class DefineRequestOrigen(BaseEstimator, TransformerMixin):
    """
    Clean the emails in a column.
    :param X: Dataframe to be used to clean the emails.
    :param column_email: Column to be used to clean the emails.
    :param column_operator: Column with the names of the operator that register the request
    :return: Dataframe with the correct origen of the request.
    """

    def __init__(self, column_operator, column_email, column_origen='origen_proceso'):
        self.column_operator = column_operator
        self.column_email = column_email
        self.column_origen = column_origen

    def fit(self, X, y=None):
        return self

    def transform(self, X=None):
        start_time = time.time()
        # Lista de los correos de los preferenciales
        df_correos_pref = pd.read_excel(
            r'E:\Inteligencia de Negocios\13. Renovaciones\INFO PREFERENCIALES.xlsx',
            sheet_name='Hoja1'
        )
        df_correos_pref['CORREOS CLIENTES '].str.strip()
        set_correos_pref = set(df_correos_pref['CORREOS CLIENTES '])

        # Paso 2: Definir las condiciones
        conditions = [
            X[self.column_operator].isna(),
            X[self.column_email].isin(set_correos_pref),
            X[self.column_operator].str.contains('OPE_AGENTE', na=False),
            ~X[self.column_operator].str.contains('OPE_SECDATA|OPE_SD', na=False)
        ]
        # Paso 3: Definir las opciones
        choices = [
            'Security Data',
            'Preferenciales',  # Para la primera condición
            'AGENTE',   # Para la condicion de agentes
            'Terceros'  # Para la condicion de Terceros
        ]

        # Paso 4: Usar np.select para asignar los valores según las condiciones
        X[self.column_origen] = np.select(
            conditions,  # Lista de condiciones
            choices,  # Lista de valores correspondientes
            default='Security Data'  # Valor por defecto si ninguna condición es True
        )

        print(f"Tiempo de ejecucion DefineRequestOrigen {time.time() - start_time}")
        return X


class VerificarPeriodoRenovacion(BaseEstimator, TransformerMixin):
    """
    Verify if a signature is a renewal.
    :param X: Dataframe to be used to verify the renewal.
    :param column_date_init: Column to be used to verify the renewal.
    :param column_date_expiration: Column name to be added.
    :param column_key: Column Key to Group Request from the same client
    :return: Dataframe with the renewal verified.
    """

    def __init__(self, column_date_init, column_date_expiration, column_key, producto, vigencia, add_column='status'):
        self.column_date_init = column_date_init
        self.column_date_expiration = column_date_expiration
        self.column_key = column_key
        self.add_column = add_column
        self.producto = producto
        self.vigencia = vigencia

    def fit(self, X, y=None):
        return self

    def transform(self, X=None):
        start_time = time.time()
        # Ordenar el dataframe por la columna de la fecha de inicio y Fecha de inicio del proceso
        X.sort_values(by=[self.column_key, self.column_date_init], inplace=True)
        X.reset_index(drop=True, inplace=True)

        # Agregar la columna de Estatus
        X[self.add_column] = 'Renovado'
        df_ultimos = X.groupby(self.column_key, group_keys=False).tail(1)
        #df_ultimos['valor_especifico'] = 'No Renovados'
        X.loc[df_ultimos.index, self.add_column] = 'No Renovado'

        # Iterar sobre los registros agrupados por la columna key
        # Proceso de Revision del periodo de renovacion
        # En caso de existir un unico registro, o ser el ultimo registro del mismo key, se asume que no se puede
        # determinar si es una renovacion anticipada o no
        # Recorremos una posicion posterior todas las filas de fecha de expiracion con la finalidad de poder comparar
        # la fecha de inicio del siguiente tramite con la fecha de expiracion del tramite actual
        X['fecha_ini_tram_reno'] = X[self.column_date_init].copy()
        X['fecha_ini_tram_reno'] = X.groupby(self.column_key)['fecha_ini_tram_reno'].shift(-1)

        # Identificar la el origen  de la renovacion
        X['origen_proceso_reno'] = X['origen_proceso'].copy()
        X['origen_proceso_reno'] = X.groupby(self.column_key)['origen_proceso_reno'].shift(-1)

        # Identificar la el origen  de la renovacion
        X['origen_test_delete'] = X['origen_proceso'].copy()
        X['origen_test_delete'] = X.groupby(self.column_key)['origen_test_delete'].shift(1)

        # Fecha de cducidad de la firma previa
        X['fecha_caducidad_previa'] = X[self.column_date_expiration].copy()
        X['fecha_caducidad_previa'] = X.groupby(self.column_key)['fecha_caducidad_previa'].shift(1)
        X['fecha_caducidad_previa'] = pd.to_datetime(X['fecha_caducidad_previa'])

        # Identificar Tipo de atencion de la renovacion
        X['atention_reno'] = X['atencion'].copy()
        X['atention_reno'] = X.groupby(self.column_key)['atention_reno'].shift(-1)

        # Identificar la Vigencia de la renovacion
        X['vigencia_reno'] = X['vigencia'].copy()
        X['vigencia_reno'] = X.groupby(self.column_key)['vigencia_reno'].shift(-1)

        # Calcular la diferencia en días
        X['diferencia_dias'] = (X['fecha_ini_tram_reno'] - X['fecha_caducidad']).dt.days

        #vigencia_anios = ['1', '2', '3', '4', '5', '6']
        # Indicar las condiciones de renovacion y los plazos
        conditions = [
            (X["fecha_ini_tram_reno"] < X[self.column_date_expiration] - timedelta(days=90)),
            (X['fecha_ini_tram_reno'] < X[self.column_date_expiration]),
            (X['fecha_ini_tram_reno'] <= X[self.column_date_expiration] + timedelta(days=30)),
            ((X[self.add_column] == 'No Renovado') & (X['max_fecha_caducidad'] > X['fecha_caducidad'])),
            X['fecha_ini_tram_reno'].notna()
        ]

        choices = [
            'Duplica Su Firma Vigente',
            'Firma Ren. Anticipada 90 dias',
            'Firma Ren. Plazo 30 dias',
            'Firma No Renovada, Tiene Firma Vigente',
            'Firma Ren. Fuera Periodo'
        ]
        X['Estado Firma Caducada'] = np.select(conditions, choices, default='Firma No Renovada')
        X['Mom. de renovacion'] = X.groupby(self.column_key)['Estado Firma Caducada'].shift(1)

        condition_mes_reno = [
            X['fecha_ini_tram_reno'].isna(),

            (X['fecha_ini_tram_reno'].dt.month == X[self.column_date_expiration].dt.month) &
            (X['fecha_ini_tram_reno'].dt.year == X[self.column_date_expiration].dt.year) &
            (
                    ((X['origen_proceso_reno'] == 'Security Data') & X['origen_proceso'] == 'Security Data') |
                    ((X['origen_proceso_reno'].isin(['Terceros', 'Preferenciales', 'Security Data'])) & (
                            X['origen_proceso'] == 'Security Data'))
            ) & (X['vigencia'].isin(['1', '2', '3', '4', '5', '6']))
            #& (X['fecha_aprobacion'].dt.year < X['fecha_ini_tram_reno'].dt.year)
            #& (X['producto'].isin(['Renovación', 'Emisión']))
            ,

            ((X['fecha_ini_tram_reno'].dt.month != X[self.column_date_expiration].dt.month) |
             (X['fecha_ini_tram_reno'].dt.year != X[self.column_date_expiration].dt.year)) &
            (
                    ((X['origen_proceso_reno'] == 'Security Data') & X['origen_proceso'] == 'Security Data') |
                    ((X['origen_proceso_reno'].isin(['Terceros', 'Preferenciales', 'Security Data'])) & (
                            X['origen_proceso'] == 'Security Data'))
            ) & (X['vigencia'].isin(['1', '2', '3', '4', '5', '6']))
            #& (X['fecha_aprobacion'].dt.year < X['fecha_ini_tram_reno'].dt.year)
            #& (X['producto'].isin(['Renovación', 'Emisión']))
        ]

        choices_mes_reno = [
            'No Renueva',
            'Renovo dentro del mismo Mes',
            'Mes de Renovacion'
        ]

        X['Mes de Renovacion Ori'] = np.select(condition_mes_reno, choices_mes_reno, default='')
        X['Mes de Renovacion'] = X.groupby(self.column_key)['Mes de Renovacion Ori'].shift(1)

        print(f"Tiempo de ejecucion VerificarPeriodoRenovacion {time.time() - start_time}")
        X.reset_index(inplace=True, drop=True)

        return X
