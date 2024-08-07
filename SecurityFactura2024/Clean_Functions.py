import pandas as pd


def expiration_date_definition(df, date_activation, date_expiration, date_approval, time_validity,
                               columns_str=['ruc_pn', 'cedula']):
    # This function will define the expiration date of the product
    # La fecha de caducidad esta definida por los siguientes criterios:
    # - Si la fecha de expiracion es nula, la fecha de activacion es valida y contiene una M dentro de anos indicando que tiene una vigencia de meses se le sumaran los meses a la fecha de activacion
    # - Si la fecha de expiracion es nula, la fecha de activacion es valida y NO contiene una M dentro de anos, se le sumaran los años de vigencia a la fecha de activacion
    # - Si la fecha de expiracion es nula, la fecha de activacion es invalida, se le sumaran los años de vigencia a la fecha de aprobacion(Considerar vigencias de meses y anos)
    # - Si la fecha de caducidad es valida, se mantiene la fecha de caducidad

    #Para mantener columnas especificas en formato de str
    df[columns_str] = df[columns_str].astype(str)

    # Convertir las columnas de fecha a datetime, si no se puede convertir se le asigna un valor nulo
    df[date_activation] = pd.to_datetime(df[date_activation], errors='coerce')
    df[date_expiration] = pd.to_datetime(df[date_expiration], errors='coerce')
    df[date_approval] = pd.to_datetime(df[date_approval], errors='coerce')

    # Condiciones a ser evaluadas
    condition_1 = (df[date_expiration].isnull()
                   & df[time_validity].astype(str).str.contains('M')
                   & df[date_activation].notnull())

    condition_2 = (df[date_expiration].isnull()
                   & ~df[time_validity].astype(str).str.contains('M')
                   & df[date_activation].notnull())

    condition_3 = (df[date_expiration].isnull()
                   & df[time_validity].astype(str).str.contains('M')
                   & df[date_activation].isnull())

    condition_4 = (df[date_expiration].isnull()
                   & ~df[time_validity].astype(str).str.contains('M')
                   & df[date_activation].isnull())

    condition_5 = (df[date_expiration].notnull())

    # Paso 3: Realizar la suma con las columnas de tipo datetime
    df.loc[condition_1, date_expiration] = df.loc[condition_1, [date_activation, time_validity]].apply(
        lambda x: x[date_activation] + pd.DateOffset(months=int(x[time_validity][:-1])), axis=1)
    df.loc[condition_2, date_expiration] = df.loc[condition_2, [date_activation, time_validity]].apply(
        lambda x: x[date_activation] + pd.DateOffset(years=int(x[time_validity])), axis=1)
    df.loc[condition_3, date_expiration] = df.loc[condition_3, [date_approval, time_validity]].apply(
        lambda x: x[date_approval] + pd.DateOffset(months=int(x[time_validity][:-1])), axis=1)
    df.loc[condition_4, date_expiration] = df.loc[condition_4, [date_approval, time_validity]].apply(
        lambda x: x[date_approval] + pd.DateOffset(years=int(x[time_validity])), axis=1)
    df.loc[condition_5, date_expiration] = df[date_expiration]
    return df


def delete_test_data(dataset, column_name, string_delete=['test', 'prueb']):
    # Esta funcion eliminara los registros que contengan la palabra "test" en la columna indicada
    for word in string_delete:
        dataset = dataset[~dataset[column_name].str.contains(word, case=False)]
    return dataset


def print_hola():
    print("HOLA")
