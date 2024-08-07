import time
from datetime import timedelta

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin


class DeleteDuplicateEntries(BaseEstimator, TransformerMixin):
    """
    Delete duplicate entries in a dataframe in a column
    :param X: Dataframe to be used to delete the duplicates.
    :param column: Column to be used to delete the duplicates.
    :param keep_value: Value to keep in the duplicates.
    :return: Dataframe without duplicates.
    """

    def __init__(self, column, keep_value='first'):
        self.column = column
        self.keep_value = keep_value

    def fit(self, X):
        return self

    def transform(self, X=None):
        start_time = time.time()
        # Drop duplicates in function of null elements in the rows
        X['nulls_count'] = X.isnull().sum(axis=1)
        X.sort_values(by=[self.column, 'nulls_count'], inplace=True)
        X.drop_duplicates(subset=[self.column], keep=self.keep_value, inplace=True)
        X.reset_index(drop=True, inplace=True)
        print(f"Tiempo de ejecucion DeleteDuplicateEntries {time.time() - start_time}")
        return X


class UpdateSignatureStatus(BaseEstimator, TransformerMixin):
    """
    Update the status of the signature in a dataframe.
    :param X: Dataframe to be used to update the status.
    :param estado_tramite: Column to be used to update the status.
    :param estado_firma: Status to be updated.
    :param produto: Product to be updated.
    :return: Dataframe with the updated status.
    """

    def __init__(self, estado_tramite, estado_firma, producto):
        self.estado_tramite = estado_tramite
        self.estado_firma = estado_firma
        self.producto = producto

    def fit(self, X):
        return self

    def transform(self, X=None):
        start_time = time.time()
        X[self.estado_firma] = X.apply(self.actualizar_estado_firma, axis=1)
        print(f"Tiempo de ejecucion UpdateSignatureStatus {time.time() - start_time}")
        return X

    def actualizar_estado_firma(self, row):
        if ((pd.isna(row[self.estado_firma]) or row[self.estado_firma] == "")
                and row[self.estado_tramite] != "Finalización de Trámite"):
            return "No Aprobado"
        elif row[self.producto] == "Emision SF Sin Firma":
            return "SF sin Firma"
        else:
            return row[self.estado_firma]


class ExtractHourDateTime(BaseEstimator, TransformerMixin):
    """
    Extract the hour from a datetime column.
    :param X: Dataframe to be used to extract the hour.
    :param datetime_column: Column to be used to extract the hour.
    :param hour_column: Column to store the hour extracted.
    :return: Dataframe with the hour extracted.
    """

    def __init__(self, datetime_column, hour_column='hour'):
        self.datetime_column = datetime_column
        self.hour_column = hour_column

    def fit(self, X):
        return self

    def transform(self, X=None):
        start_time = time.time()
        X[self.hour_column] = X[self.datetime_column].dt.strftime('%H:%M:%S')
        print(f"Tiempo de ejecucion ExtractHourDateTime {time.time() - start_time}")
        return X


class DTypeInt(BaseEstimator, TransformerMixin):
    """
    Transform the type of a list column to int.
    :param X: Dataframe to be used to transform the type.
    :param column_list: List of Column to be transformed.
    """

    def __init__(self, columns):
        self.columns = columns

    def fit(self, X, y=None):
        return self

    def transform(self, X=None):
        start_time = time.time()
        for column in self.columns:
            X[column] = X[column].astype(int)
        print(f"Tiempo de ejecucion DTypeInt {time.time() - start_time}")
        return X


class DTypeFloat(BaseEstimator, TransformerMixin):
    """
    Transform the type of a list column to float.
    :param X: Dataframe to be used to transform the type.
    :param column_list: List of Column to be transformed.
    """

    def __init__(self, column):
        self.column = column

    def fit(self, X, y=None):
        return self

    def transform(self, X=None):
        start_time = time.time()
        for column in self.column:
            X[column] = X[column].astype(float)
        print(f"Tiempo de ejecucion DTypeFloat {time.time() - start_time}")
        return X


class DTypeObject(BaseEstimator, TransformerMixin):
    """
    Transform the type of a list column to object.
    :param X: Dataframe to be used to transform the type.
    :param column_list: List of Column to be transformed.
    """

    def __init__(self, column):
        self.column = column

    def fit(self, X, y=None):
        return self

    def transform(self, X=None):
        start_time = time.time()
        for column in self.column:
            X[column] = X[column].astype(object)
        print(f"Tiempo de ejecucion DTypeObject {time.time() - start_time}")
        return X


class DeleteColumns(BaseEstimator, TransformerMixin):
    """
    Transform the type of a list column to object.
    :param X: Dataframe to be used to transform the type.
    :param column_list: List of Column to be transformed.
    """

    def __init__(self, columns):
        self.columns = columns

    def fit(self, X, y=None):
        return self

    def transform(self, X=None):
        start_time = time.time()
        X.drop(self.columns, axis=1, inplace=True)
        print(f"Tiempo de ejecucion DeleteColumns {time.time() - start_time}")
        return X


class DeleteNullEntries(BaseEstimator, TransformerMixin):
    """
    Delete rows with null values.
    :param X: Dataframe to be used to delete the rows.
    :param column_list: List of Column to be transformed.
    """

    def __init__(self, columns):
        self.columns = columns

    def fit(self, X, y=None):
        return self

    def transform(self, X=None):
        start_time = time.time()
        X.dropna(subset=self.columns, inplace=True, axis=0)
        print(f"Tiempo de ejecucion DeleteNullEntries {time.time() - start_time}")
        return X


class BooleanToString(BaseEstimator, TransformerMixin):
    """
    Transform the Boolean True/False to words Si o No.
    :param X: Dataframe to be used to transform the Boolean.
    :param column: Column to be transformed.
    """

    def __init__(self, column):
        self.column = column

    def fit(self, X, y=None):
        return self

    def transform(self, X=None):
        start_time = time.time()
        X[self.column] = X[self.column].apply(lambda x: 'Si' if x else 'No')
        print(f"Tiempo de ejecucion BooleanToString {time.time() - start_time}")
        return X


class ReplaceValues(BaseEstimator, TransformerMixin):
    """
    Replace values in a column.
    :param X: Dataframe to be used to replace the values.
    :param column: Column to be replaced.
    :param old_value: List Value to be replaced.
    :param new_value: List New value.
    :return: Dataframe with the values replaced.
    """

    def __init__(self, column, old_value, new_value):
        self.column = column
        self.old_value = old_value
        self.new_value = new_value

    def fit(self, X, y=None):
        return self

    def transform(self, X=None):
        start_time = time.time()
        for new_value, old_value in zip(self.new_value, self.old_value):
            X[self.column] = X[self.column].replace(old_value, new_value)
        print(f"Tiempo de ejecucion ReplaceValues {time.time() - start_time}")
        return X


class FilterPerDate(BaseEstimator, TransformerMixin):
    """
    Filter the dataframe by date.
    :param X: Dataframe to be used to filter by date.
    :param date_column: Column to be used to filter by date.
    :param start_date: Start date to filter.
    :param end_date: End date to filter.
    :return: Dataframe filtered by date.
    """

    def __init__(self, date_column, start_date, end_date=None):
        self.date_column = date_column
        self.start_date = start_date
        self.end_date = end_date

    def fit(self, X, y=None):
        return self

    def transform(self, X=None):
        start_time = time.time()
        X[self.date_column] = X[self.date_column].fillna(X['fecha_caducidad_mod'])
        if self.end_date is None:
            X = X[(X[self.date_column] >= self.start_date)].reset_index(drop=True)
        else:
            X = X[(X[self.date_column] >= self.start_date) & (X[self.date_column] <= self.end_date)].reset_index(
                drop=True)
        print(f"Tiempo de ejecucion FilterPerDate {time.time() - start_time}")
        return X


class FilterPerListMatchs(BaseEstimator, TransformerMixin):
    """
    Filter the dataframe by a lists Matchs.
    :param X: Dataframe to be used to filter by date.
    :param match_column: Column to be used to filter.
    :param match_list: Listado de Items con los cuales quiero hacer matchs para conservar
    :return: Dataframe filtered by date.
    """

    def __init__(self, match_column, match_list):
        self.match_column = match_column
        self.match_list = match_list

    def fit(self, X, y=None):
        return self

    def transform(self, X=None):
        start_time = time.time()
        # Filtrar los elementos que están en match_list
        X = X[X[self.match_column].isin(self.match_list)].reset_index(drop=True)
        print(f"Tiempo de ejecucion FilterPerListMatchs {time.time() - start_time}")
        return X


class FilterPerNotListMatchs(BaseEstimator, TransformerMixin):
    """
    Filter the dataframe by a lists Matchs.
    :param X: Dataframe to be used to filter by date.
    :param match_column: Column to be used to filter.
    :param match_list: Listado de Items con los cuales quiero hacer matchs para eliminar
    :return: Dataframe filtered by date.
    """

    def __init__(self, match_column, match_list):
        self.match_column = match_column
        self.match_list = match_list

    def fit(self, X, y=None):
        return self

    def transform(self, X=None):
        start_time = time.time()
        # Filtrar los elementos que están en match_list
        X[~X[self.match_column].isin(self.match_list)].reset_index(drop=True)
        print(f"Tiempo de ejecucion FilterPerNotListMatchs {time.time() - start_time}")
        return X


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
        X = pd.concat([self.df1, self.df2], axis=self.axis)
        print(f"Tiempo de ejecucion ConcatenatedDataFrames {time.time() - start_time}")
        return X


class DeterminarFirmasCaducadas(BaseEstimator, TransformerMixin):
    """
    Determine the signatures that have expired.
    :param X: Dataframe to be used to determine the signatures.
    :param column: Column to be used to determine the signatures.
    :return: Dataframe with the signatures determined.
    """

    def __init__(self, column_date, column_estado_firma='estado_firma'):
        self.column_date = column_date
        self.column_estado_firma = column_estado_firma

    def fit(self, X, y=None):
        return self

    def transform(self, X=None):
        start_time = time.time()
        # Convertir la fecha actual al formato de pandas
        today = pd.to_datetime('today').normalize()

        #X[self.column_date] = X[self.column_date].str.split(' ').str[0]
        # Convertir la columna a datetime con formato mixto
        X[self.column_date] = pd.to_datetime(X[self.column_date], format='mixed')
        # Aplicar la condición y actualizar la columna 'column_estado_firma'
        X.loc[X[self.column_date] < today, self.column_estado_firma] = 'Caducado'
        print(f"Tiempo de ejecucion DeterminarFirmasCaducadas {time.time() - start_time}")
        return X


class AgregarColumnaValor(BaseEstimator, TransformerMixin):
    """
    Add a column with a value.
    :param X: Dataframe to be used to add the column.
    :param column_name: Column name to be added.
    :param value: Value to be added.
    :return: Dataframe with the column added.
    """

    def __init__(self, column_name, value):
        self.column_name = column_name
        self.value = value

    def fit(self, X, y=None):
        return self

    def transform(self, X=None):
        start_time = time.time()
        X[self.column_name] = self.value
        print(f"Tiempo de ejecucion AgregarColumnaValor {time.time() - start_time}")
        return X


class CrearKeyWithCedulaRucTP(BaseEstimator, TransformerMixin):
    """
    Create a key with the values of the columns.
    :param X: Dataframe to be used to create the key.
    :param columns: Columns to be used to create the key.
    :param key_name: Name of the key.
    :return: Dataframe with the key created.
    """

    def __init__(self, cedula_column, ruc_column, tipo_persona_column, key_name_column):
        self.cedula_column = cedula_column
        self.ruc_column = ruc_column
        self.tipo_persona_column = tipo_persona_column
        self.key_name_column = key_name_column

    def fit(self, X, y=None):
        return self

    def transform(self, X=None):
        start_time = time.time()
        X = X.dropna(subset=[self.tipo_persona_column])
        X['ruc_copy'] = X[self.ruc_column].copy()
        X['ruc_copy'] = X['ruc_copy'].fillna(X[self.cedula_column].astype(str) + "001")
        X[self.key_name_column] = X[[self.cedula_column, 'ruc_copy', self.tipo_persona_column]].agg(''.join, axis=1)
        print(f"Tiempo de ejecucion CrearKeyWithCOlumns {time.time() - start_time}")
        return X


class IdentificarMaxDateGroups(BaseEstimator, TransformerMixin):
    """
    Identify the maximum date in a column.
    :param X: Dataframe to be used to identify the maximum date.
    :param date_column: Column to be used to identify the maximum date.
    :param group_columns: Columns to be used to group the maximum date.
    :param add_column: Column to be added with the maximum date or in this case with a clause
    :return: Dataframe with the maximum date identified.
    """

    def __init__(self, date_column, group_columns):
        self.date_column = date_column
        self.group_columns = group_columns

    def fit(self, X, y=None):
        return self

    def transform(self, X=None):
        start_time = time.time()
        # Convertir la columna a datetime con formato mixto. EN este caso no se realiza porque en
        # un proceso previo ya se transformo la fecha de caducidad como datetime, pero se debe tener en cuenta
        # para futuros procesos relaizar una transformacion de todos los tipos de datos
        # Ecnontrar la fecha mas reciente de cada key
        X['max_fecha_caducidad'] = X.groupby(self.group_columns)[self.date_column].transform('max')

        print(f"Tiempo de ejecucion IdentificarMaxDateGroups {time.time() - start_time}")
        return X


class IdentificarMinDateGroups(BaseEstimator, TransformerMixin):
    """
    Identify the maximum date in a column.
    :param X: Dataframe to be used to identify the minimum date.
    :param date_column: Column to be used to identify the minimum date.
    :param group_columns: Columns to be used to group the minimum date.
    :param add_column: Column to be added with the maximum date or in this case with a clause
    :return: Dataframe with the maximum date identified.
    """

    def __init__(self, date_column, group_columns, add_column='producto_control'):
        self.date_column = date_column
        self.group_columns = group_columns
        self.add_column = add_column

    def fit(self, X, y=None):
        return self

    def transform(self, X=None):
        start_time = time.time()
        # Convertir la columna a datetime con formato mixto. EN este caso no se realiza porque en
        # un proceso previo ya se transformo la fecha de caducidad como datetime, pero se debe tener en cuenta
        # para futuros procesos relaizar una transformacion de todos los tipos de datos
        # Ecnontrar la fecha mas reciente de cada key
        X['min_fecha_init_tramite'] = X.groupby(self.group_columns)[self.date_column].transform('min')
        # Agregar la columna con la condicion
        X[self.add_column] = 'Renovacion'
        boolean_column = (X[self.date_column] == X['min_fecha_init_tramite'])
        X.loc[boolean_column, self.add_column] = 'Emision'
        #X[self.add_column] = X[self.add_column].apply(lambda x: 'No Renovado' if x else 'Renovado')
        print(f"Tiempo de ejecucion IdentificarMinDateGroups {time.time() - start_time}")
        return X


class OrdenarDataFramePorColumnas(BaseEstimator, TransformerMixin):
    """
    Order the dataframe by columns.
    :param X: Dataframe to be used to order by columns.
    :param columns: Columns to be used to order the dataframe.
    :return: Dataframe ordered by columns.
    """

    def __init__(self, columns, ascending=True):
        self.columns = columns
        self.ascending = ascending

    def fit(self, X, y=None):
        return self

    def transform(self, X=None):
        start_time = time.time()
        X.sort_values(by=self.columns, inplace=True, ascending=self.ascending)
        X.reset_index(drop=True, inplace=True)
        print(f"Tiempo de ejecucion OrdenarDataFramePorColumnas {time.time() - start_time}")
        return X


class DTypeDateTime(BaseEstimator, TransformerMixin):
    """
    Transform the type of a list column to datetime.
    :param X: Dataframe to be used to transform the type.
    :param column_list: List of Column to be transformed.
    """

    def __init__(self, columns):
        self.columns = columns

    def fit(self, X, y=None):
        return self

    def transform(self, X=None):
        start_time = time.time()
        for column in self.columns:
            X[column] = pd.to_datetime(X[column], format='mixed', errors='coerce')
        print(f"Tiempo de ejecucion DTypeDateTime {time.time() - start_time}")
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

    def __init__(self, column_date_init, column_date_expiration, column_key, add_column='status'):
        self.column_date_init = column_date_init
        self.column_date_expiration = column_date_expiration
        self.column_key = column_key
        self.add_column = add_column

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

        X['atention_reno'] = X['atencion'].copy()
        X['atention_reno'] = X.groupby(self.column_key)['atention_reno'].shift(-1)
        # Calcular la diferencia en días
        X['diferencia_dias'] = (X['fecha_ini_tram_reno'] - X['fecha_caducidad']).dt.days

        #vigencia_anios = ['1', '2', '3', '4', '5', '6']
        # Indicar las condiciones de renovacion y los plazos
        conditions = [
            (X['fecha_ini_tram_reno'] < X[self.column_date_expiration]),
            (X['fecha_ini_tram_reno'] <= X[self.column_date_expiration] + timedelta(days=30)),
            ((X[self.add_column] == 'No Renovado') & (X['max_fecha_caducidad'] > X['fecha_caducidad'])),
            X['fecha_ini_tram_reno'].notna()
            # Esta condición cubre todos los casos restantes que no han sido capturados
        ]

        choices = [
            'Ren. Anticipada',
            'Ren. Plazo 30 dias',
            'No Renovada Con Firma Vigente',
            'Ren. Fuera Periodo'
        ]

        X['Momento  de la Renovacion'] = np.select(conditions, choices, default='No Renueva')

        print(f"Tiempo de ejecucion VerificarPeriodoRenovacion {time.time() - start_time}")

        return X


class EmptyMethod(BaseEstimator, TransformerMixin):
    """
    Transform the type of a list column to datetime.
    :param X: Dataframe to be used to transform the type.
    :param column_list: List of Column to be transformed.
    """

    def __init__(self, columns=None):
        self.columns = columns

    def fit(self, X, y=None):
        return self

    def transform(self, X=None):
        return X


class LimpiezaCorreosPref(BaseEstimator, TransformerMixin):
    """
    Clean the emails in a column.
    :param X: Dataframe to be used to clean the emails.
    :param column: Column to be used to clean the emails.
    :return: Dataframe with the emails cleaned.
    """

    def __init__(self, column):
        self.column = column

    def fit(self, X, y=None):
        return self

    def transform(self, X=None):
        start_time = time.time()
        # Lista de los correos de los preferenciales
        df_correos_pref = pd.read_excel(
            r'E:\Inteligencia de Negocios\13. Renovaciones\INFO PREFERENCIALES',
            sheet_name='Hoja1'
        )
        df_correos_pref['CORREOS CLIENTES '].str.strip()
        set_correos_pref = set(df_correos_pref['CORREOS CLIENTES '])
        X = X[~X[self.column].isin(set_correos_pref)].reset_index(drop=True)
        print(f"Tiempo de ejecucion LimpiezaCorreosPref {time.time() - start_time}")
        return X