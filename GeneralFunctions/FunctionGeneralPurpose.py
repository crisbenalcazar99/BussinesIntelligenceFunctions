import os
import re
import time
from datetime import timedelta, datetime

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

    def __init__(self, column, nulls_count, keep_value='first'):
        self.column = column
        self.nulls_count = nulls_count
        self.keep_value = keep_value

    def fit(self, X):
        return self

    def transform(self, X=None):
        start_time = time.time()

        # Ordenar el DataFrame por la columna clave y el conteo de nulos
        X.sort_values(by=[self.column, self.nulls_count], inplace=True, ascending=True)

        # Elimina los elementos Duplicados en funcion de la columna manteniendo el primer registro
        X = X.drop_duplicates(subset=[self.column], keep=self.keep_value)
        X.reset_index(drop=True, inplace=True)

        print(f"Tiempo de ejecucion DeleteDuplicateEntries {time.time() - start_time}")
        return X


class CountNullValuesRow(BaseEstimator, TransformerMixin):
    """
    Delete duplicate entries in a dataframe in a column
    :param X: Dataframe to be used to delete the duplicates.
    :param column: Column to be used to delete the duplicates.
    :param keep_value: Value to keep in the duplicates.
    :return: Dataframe without duplicates.
    """

    def __init__(self, new_column_name):
        self.new_column_name = new_column_name

    def fit(self, X):
        return self

    def transform(self, X=None):
        start_time = time.time()

        # Calcular la cantidad de valores nulos usando NumPy
        X[self.new_column_name] = np.sum(pd.isnull(X).values, axis=1)

        print(f"Tiempo de ejecucion Count Null Rows {time.time() - start_time}")
        return X


class DeleteDuplicateEntriesWithNulls(BaseEstimator, TransformerMixin):
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

        # Calcular la cantidad de valores nulos usando NumPy
        X['nulls_count'] = np.sum(pd.isnull(X).values, axis=1)

        # Ordenar el DataFrame por la columna clave y el conteo de nulos
        X.sort_values(by=[self.column, 'nulls_count'], inplace=True, ascending=True)

        # Filtrar filas donde self.column no es nulo
        X_non_null = X.dropna(subset=[self.column])

        # Obtener los índices únicos usando numpy para el valor en self.column
        _, unique_indices = np.unique(X_non_null[self.column].values, return_index=True)
        X_non_null_unique = X_non_null.iloc[unique_indices]

        # Filtrar filas donde self.column es nulo
        X_null = X[X[self.column].isna()]

        # Combinar ambos DataFrames (filas no nulas procesadas + filas nulas originales)
        X = pd.concat([X_non_null_unique, X_null], ignore_index=True)

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

    def __init__(self, columns):
        self.columns = columns

    def fit(self, X, y=None):
        return self

    def transform(self, X=None):
        start_time = time.time()
        for column in self.columns:
            # Reemplazar cadenas vacías con NaN para manejar correctamente los valores nulos
            X[column] = X[column].replace('', np.nan)

            # Convertir la columna a tipo float, ignorando errores de conversión
            X[column] = pd.to_numeric(X[column], errors='coerce')
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
        if self.end_date is None:
            X = X[(X[self.date_column] >= self.start_date)].reset_index(drop=True)
        else:
            X = X[(X[self.date_column] >= self.start_date) & (X[self.date_column] <= self.end_date)].reset_index(
                drop=True)
        print(f"Tiempo de ejecucion FilterPerDate {time.time() - start_time}")
        return X


class FillNAValues(BaseEstimator, TransformerMixin):
    """
    Fill NA Values in the dataframe with other column values.
    :param X: Dataframe to be used to filter by date.
    :param base_column: Column with NA Values that will be fill.
    :param auxiliar_column: Values column to be used to fill base_column.
    :return: Dataframe filtered by date.
    """

    def __init__(self, base_column, auxiliar_column):
        self.base_column = base_column
        self.auxiliar_column = auxiliar_column

    def fit(self, X, y=None):
        return self

    def transform(self, X=None):
        start_time = time.time()
        X[self.base_column] = X[self.base_column].fillna(X[self.auxiliar_column])
        print(f"Tiempo de ejecucion Fill NA Values in Column {time.time() - start_time}")
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
    Filter the dataframe by a lists Matching elements that not be in the list.
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
        X[~X[self.match_column].isin(self.match_list)].reset_index(drop=True, inplace=True)
        print(f"Tiempo de ejecucion FilterPerNotListMatchs {time.time() - start_time}")
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
        print('Antes de eliminar tipo_persona_column')
        X = X.dropna(subset=[self.tipo_persona_column])
        # Copiar la columna 'ruc' sin que se convierta a float
        X['ruc_copy'] = X[self.ruc_column].astype(str)

        # Reemplazar NaN por una cadena específica (cedula + "001")
        X['ruc_copy'] = np.where(X['ruc_copy'].isin(['nan', 'NaN', 'None', '']),
                                 X[self.cedula_column].astype(str) + "001",
                                 X['ruc_copy'])

        # Asegurarse de que la columna es de tipo string
        X['ruc_copy'] = X['ruc_copy'].astype(str)

        X[self.key_name_column] = X[[self.cedula_column, 'ruc_copy', self.tipo_persona_column]].agg(''.join, axis=1)
        print('Despues de eliminar tipo_persona_column')
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


class ChangeDateInitTramFact(BaseEstimator, TransformerMixin):
    """
    Transform the type of a list column to datetime.
    :param X: Dataframe to be used to transform the type.
    :param column_list: List of Column to be transformed.
    """

    def __init__(self, column_date_init, column_date_fact):
        self.column_date_init = column_date_init
        self.column_date_fact = column_date_fact

    def fit(self, X, y=None):
        return self

    def transform(self, X=None):
        start_time = time.time()
        X[self.column_date_init] = X[self.column_date_fact].fillna(X[self.column_date_init])
        print(f"Tiempo de ejecucion ChangeDateInitTramFact {time.time() - start_time}")
        return X


class ExtractNumerateRows(BaseEstimator, TransformerMixin):
    def __init__(self, mes_renovacion, producto, vigencia, mom_renovacion, valor_factura):
        self.mes_renovacion = mes_renovacion
        self.producto = producto
        self.vigencia = vigencia
        self.mom_renovacion = mom_renovacion
        self.valor_factura = valor_factura

    def fit(self, X, y=None):
        return self

    def transform(self, X=None):
        start_time = time.time()

        # Extraer mes y año en formato MM/AAAA
        X['mes_ano_aprob'] = X['fecha_aprobacion'].dt.strftime('%m%Y')
        X['mes_ano_cadu'] = X['fecha_caducidad'].dt.strftime('%m%Y')

        # Filtrar las filas en función de la condición
        X_extract = X[
            X['Mes de Renovacion'].isin(['Renovo dentro del mismo Mes', 'Mes de Renovacion']) &
            X['medio_contacto'].isin(['Whatsapp', 'Mailing', 'Llamada del operador', 'Medios'])
            ]

        # Ordenar por mes_ano_aprob y valor_factura en orden ascendente para que las facturas con menor valor sean numeradas primero
        X_extract.sort_values(by=['mes_ano_aprob', self.valor_factura], ascending=[True, False], inplace=True)

        # Numerar las filas agrupándolas por mes_ano_aprob
        X.loc[X_extract.index, 'numero_fila'] = X_extract.groupby('mes_ano_aprob').cumcount() + 1
        '''
        # Filtrado del DataFrame basado en las condiciones
        filtro = (
                X['vigencia'].isin(['1', '2', '3', '4', '5', '6']) &
                X['medio_contacto'].isin(['Whatsapp', 'Mailing', 'Llamada del Operador', 'Medios']) &
                X['producto'].isin(['Agregar RUC a Firma', 'Emisión', 'Recuperacion Clave', 'Renovación']) &
                X['origen_proceso'].isin(['Security Data']) &
                ~X['Estado Firma Caducada'].isin(['Firma No Renovada, Tiene Firma Vigente'])
        )

        # Aplicar el filtro de las condiciones previas al DataFrame
        X_filtrado = X[filtro]

        # Contar la cantidad de firmas caducadas agrupadas por 'mes_ano_aprob'
        X_filtrado['cantidad_firmas_caducadas'] = X_filtrado.groupby('mes_ano_aprob')[
            'Estado Firma Caducada'].transform('count')

        # Rellenar valores nulos con 0 en el DataFrame filtrado
        #X_filtrado['cantidad_firmas_caducadas'].fillna(0, inplace=True)

        # Hacer un merge con el DataFrame original para agregar la nueva columna
        X = X.merge(X_filtrado[['mes_ano_aprob', 'cantidad_firmas_caducadas']], on='mes_ano_aprob', how='left')

        # Rellenar los valores nulos en la columna 'cantidad_firmas_caducadas' del DataFrame original con 0
        X['cantidad_firmas_caducadas'].fillna(0, inplace=True)
        '''
        X['test_delete1'] = 'hola'
        X['numero_fila'].fillna('0', inplace=True)
        X['test_delete'] = 'hola'

        print(f'El tiempo total de la funcionChangeDateInitTramFact es de {time.time() - start_time}')
        return X


class ValueToComisionar(BaseEstimator, TransformerMixin):
    """
    Transform the type of a list column to datetime.
    :param X: Dataframe to be used to transform the type.
    :param column_list: List of Column to be transformed.
    """

    def __init__(self, valor_factura, vigencia, valor_factura_comision, renovacion):
        self.valor_factura = valor_factura
        self.vigencia = vigencia
        self.valor_factura_comision = valor_factura_comision
        self.renovacion = renovacion

    def fit(self, X, y=None):
        return self

    def transform(self, X=None):
        start_time = time.time()
        precios_firmas = [

            22.43,  # 1 año - nuevo
            34.85,  # 2 años - nuevo
            48.92,  # 3 años - nuevo
            61.34,  # 4 años - nuevo
            72.11,  # 5 años - nuevo
            18.40,  # 1 año - renovación
            27.60,  # 2 años - renovación
            39.33,  # 3 años - renovación
            49.68,  # 4 años - renovación
            58.65,  # 5 años - renovación
            23.0,  # 1 ano Renovacion Token
            35.84,  # 2 anos Renovacion Token,
            22.40,  # 1 ano Renovacion Token
            48.3,  # Token 1 Ano
            64.9,  # Descuento 10 % de Renovacion 5 anos
            55.21,  # Descuento 10 % de Renovacion 4 anos
            44.03,  # Descuento 10 % de Renovacion 3 anos
            31.36,  # Descuento 10 % de Renovacion 2 anos
            20.18,  # Descuento 10 % de Renovacion 5 anos
            68.5,  # ONLINE-DESC-SD 5% Online 5 anos
            58.27,  # ONLINE-DESC-SD 5% 4 anos
            46.47,  # ONLINE-DESC-SD 5% Online 3 anos
            33.1,  # ONLINE-DESC-SD Online 2 anos
            21.30  # ONLINE-DESC-SD Online 1 anos

        ]
        conditions = [
            (X[self.valor_factura].isin(precios_firmas)),
            ((X[self.vigencia] == '1') & (X[self.renovacion].isin(['Renovación']))),
            ((X[self.vigencia] == '1') & (X[self.renovacion].isin(['Emisión', 'Emision SF']))),
            ((X[self.vigencia] == '2') & (X[self.renovacion].isin(['Renovación']))),
            ((X[self.vigencia] == '2') & (X[self.renovacion].isin(['Emisión', 'Emision SF']))),
            ((X[self.vigencia] == '3') & (X[self.renovacion].isin(['Renovación']))),
            ((X[self.vigencia] == '3') & (X[self.renovacion].isin(['Emisión', 'Emision SF']))),
            ((X[self.vigencia] == '4') & (X[self.renovacion].isin(['Renovación']))),
            ((X[self.vigencia] == '4') & (X[self.renovacion].isin(['Emisión', 'Emision SF']))),
            ((X[self.vigencia] == '5') & (X[self.renovacion].isin(['Renovación']))),
            ((X[self.vigencia] == '5') & (X[self.renovacion].isin(['Emisión', 'Emision SF']))),
            ((X[self.vigencia] == '1M') & (X[self.renovacion].isin(['Emisión', 'Emision SF']))),
            ((X[self.vigencia] == '1S') & (X[self.renovacion].isin(['Emisión', 'Emision SF'])))
        ]

        choices = [
            X[self.valor_factura],  # Si el valor ya está en la lista de precios
            18.40,  # 1 año - renovación
            22.43,  # 1 año - nuevo
            27.60,  # 2 años - renovación
            34.85,  # 2 años - nuevo
            39.33,  # 3 años - renovación
            48.92,  # 3 años - nuevo
            49.68,  # 4 años - renovación
            61.34,  # 4 años - nuevo
            58.65,  # 5 años - renovación
            72.11,  # 5 años - nuevo
            12.10,  # 1 mes - nuevo
            5.15  # 1 semana - nuevo
        ]

        # Asignar los valores usando np.select
        X[self.valor_factura_comision] = np.select(conditions, choices, default=None)
        print(f'El tiempo total de ejecucion de la fucnion ValueToComisionar es de {time.time() - start_time}')
        return X


class CSVLoaderTransformer(BaseEstimator, TransformerMixin):
    def __init__(self, file_path):
        self.file_path = file_path

    def fit(self, X=None, y=None):
        # No necesita ajuste, simplemente devuelve self
        return self

    def transform(self, X=None):
        # Definir las columnas que deseas cargar como cadenas de texto
        columns_to_str = ['ruc', 'cedula', 'vigencia']

        # Cargar el DataFrame desde el archivo CSV, especificando el tipo de datos para las columnas
        df = pd.read_csv(self.file_path, dtype={col: str for col in columns_to_str})

        # Definir las columnas que deseas convertir a datetime
        columns_to_check = [
            'fecha_caducidad', 'fecha_inicio_tramite', 'fecha_fin_tramite',
            'fecha_aprobacion', 'fecha_factura', 'fecha_expedicion'
        ]

        # Intentar convertir las columnas a datetime si existen en el DataFrame
        for column in columns_to_check:
            if column in df.columns:
                # Convertir la columna a str primero
                df[column] = df[column].astype(str)

                # Eliminar todo a partir de un punto incluido el punto mismo
                df[column] = df[column].apply(lambda x: re.sub(r'\..*', '', x))

                # Luego convertirla a datetime
                df[column] = pd.to_datetime(df[column], errors='coerce')
                # Definir las columnas que deseas convertir a datetime

        # Definir las columnas que deseas convertir a datetime
        columns_to_float = [
            'valorfactura', 'subtotal'
        ]

        # Intentar convertir las columnas a datetime si existen en el DataFrame
        for column in columns_to_float:
            if column in df.columns:
                # Reemplazar cualquier cadena vacía o valores que no se puedan convertir con NaN
                df[column].replace('', pd.NA, inplace=True)

                # Convertir la columna a float, ignorando errores de conversión
                df[column] = pd.to_numeric(df[column], errors='coerce')

        # Mostrar información sobre el DataFrame después de la transformación
        df.info()

        return df


class ReportComisiones:
    def __init__(self):
        pass

    def fit(self, X=None, y=None):
        # No necesita ajuste, simplemente devuelve self
        return self

    def transform(self, X):
        # Filtrar por 'Mes de Renovacion' y contar por mes/año de 'fecha_aprobacion'
        X['mes_ano_aprobacion'] = X['fecha_aprobacion'].dt.strftime('%m-%Y')
        X_aprobados = X[
            X['Mes de Renovacion'].isin(['Renovo dentro del mismo Mes', 'Mes de Renovacion']) &
            X['producto'].isin(['Emisión', 'Renovación', 'Emision SF']) &
            ~X['vigencia'].isin(['1M', '1S', '2S', '3M', '6M']) &
            X['fecha_aprobacion'].dt.year.isin([2022, 2023, 2024]) &
            ~X['Mom. de renovacion'].isin(['Firma No Renovada'])
            ].groupby('mes_ano_aprobacion').size().reset_index(name='cantidad_aprobados')

        # Filtrar por condiciones dadas y contar por mes/año de 'fecha_caducidad'
        X['mes_ano_caducidad'] = X['fecha_caducidad'].dt.strftime('%m-%Y')
        filtro = (
                X['vigencia'].isin(['1', '2', '3', '4', '5', '6']) &
                X['producto'].isin(['Agregar RUC a Firma', 'Emisión', 'Recuperacion Clave', 'Renovación']) &
                X['origen_proceso'].isin(['Security Data']) &
                ~X['Estado Firma Caducada'].isin(['Firma No Renovada, Tiene Firma Vigente'])
        )
        X_caducados = X[filtro].groupby('mes_ano_caducidad').size().reset_index(name='cantidad_caducados')

        # Unir ambos resultados y conservar solo aquellos donde mes_ano_aprobacion y mes_ano_caducidad sean iguales
        resultado = pd.merge(
            X_aprobados, X_caducados,
            left_on='mes_ano_aprobacion',
            right_on='mes_ano_caducidad',
            how='inner'  # Usamos 'inner' para conservar solo los que coinciden
        ).rename(columns={'mes_ano_aprobacion': 'mes_ano'})

        # Rellenar valores NaN con 0 en las columnas de conteo
        resultado['cantidad_aprobados'].fillna(0, inplace=True)
        resultado['cantidad_caducados'].fillna(0, inplace=True)

        # Eliminar la columna 'mes_ano_caducidad', ya que hemos unificado con 'mes_ano_aprobacion'
        resultado.drop(columns=['mes_ano_caducidad'], inplace=True)

        return resultado


class ColumnTransformer(TransformerMixin):
    def __init__(self, int_cols=None, float_cols=None, obj_cols=None, datetime_cols=None, cat_cols=None):
        self.int_cols = int_cols if int_cols is not None else []
        self.float_cols = float_cols if float_cols is not None else []
        self.obj_cols = obj_cols if obj_cols is not None else []
        self.datetime_cols = datetime_cols if datetime_cols is not None else []
        self.cat_cols = cat_cols if cat_cols is not None else []

    def fit(self, X, y=None):
        return self

    def transform(self, X):

        # Transform integer columns
        for col in self.int_cols:
            if col in X.columns:
                X[col] = pd.to_numeric(X[col], downcast='integer', errors='coerce')

        # Transform float columns
        for col in self.float_cols:
            if col in X.columns:
                X[col] = pd.to_numeric(X[col], downcast='float', errors='coerce')

        # Transform object columns
        for col in self.obj_cols:
            if col in X.columns:
                X[col] = X[col].astype(str)

        # Transform datetime columns
        for col in self.datetime_cols:
            if col in X.columns:
                X[col] = pd.to_datetime(X[col], errors='coerce', format='mixed')

        # Transform categorical columns
        for col in self.cat_cols:
            if col in X.columns:
                X[col] = X[col].astype('category')

        return X


def save_dataframe_csv(X, directory, filename=None):
    if filename is None:
        # Obtener la hora para el nombre del archivo
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'{timestamp}.csv'

    file_path = os.path.join(directory, filename)

    # Verificar si el directorio existe, y si no, crearlo
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Guardar el DataFrame como CSV
    X.to_csv(file_path, index=False)


class SaveDataFrameCSV(BaseEstimator, TransformerMixin):
    def __init__(self, directory, filename=None):
        self.directory = directory
        self.filename = filename

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        if self.filename is None:
            # Obtener la hora para el nombre del archivo
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            self.filename = f'{timestamp}.csv'

        file_path = os.path.join(self.directory, self.filename)

        # Verificar si el directorio existe, y si no, crearlo
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)

        # Guardar el DataFrame como CSV
        X.to_csv(file_path, index=False)
