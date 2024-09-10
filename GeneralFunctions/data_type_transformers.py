import re
import time
from datetime import timedelta

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin


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
            if column in X.columns:
                X[column] = X[column].astype(object)
        print(f"Tiempo de ejecucion DTypeObject {time.time() - start_time}")
        return X


class DTypeCategorical(BaseEstimator, TransformerMixin):
    """
    Transform the type of a list column to Categorical.
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
            X[column] = X[column].astype('category')
        print(f"Tiempo de ejecucion DTypeObject {time.time() - start_time}")
        return X