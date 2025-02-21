import pandas as pd
from ReporteriaDatabase import connect_to_db
from ReporteriaDatabase import cargar_archivo_en_batches


def certificadosAgosto2018():
    # Leer y limpiar data del excel CERTIFICADOS TOTAL HASTA 31 AGOSTO 2018
    print('Iniciado el proceso de ETL de CERTIFICADOS TOTAL HASTA 31 AGOSTO 2018')
    path_certificados_31_Agosto_2018 = r'E:\Inteligencia de Negocios\28. Reportería Gerencia\CERTIFICADOS TOTAL HASTA 31 AGOSTO 2018.xlsx'
    df_certificados_31_Agosto_2018 = pd.read_excel(path_certificados_31_Agosto_2018, sheet_name='certificados y tokens',
                                                   skiprows=5)
    df_certificados_31_Agosto_2018 = df_certificados_31_Agosto_2018[
        ~df_certificados_31_Agosto_2018['Enterprise/Web'].isna()]
    old_columns_name = ['Uso', 'CSP', 'CN EN EL SISTEMA', 'cedula suscriptor', 'No Años', 'OBERVACION']
    new_columns_name = ['Uso_1', 'MEDIO EMISION', 'CN en el Sistema', 'CEDULA No', 'No Anos', 'Observacion']
    delete_columns = ['Unnamed: 26', 'No', 'Uso_1']
    columns_number_comilla = ['RUC', 'CEDULA No', 'Num Factura']
    columns_int = ['Activados', 'Cantidad']
    df_certificados_31_Agosto_2018.rename(columns=dict(zip(old_columns_name, new_columns_name)), inplace=True)
    df_certificados_31_Agosto_2018.drop(delete_columns, axis=1, inplace=True)
    for column in columns_int:
        df_certificados_31_Agosto_2018[column] = df_certificados_31_Agosto_2018[column].astype(int)
    for column in columns_number_comilla:
        df_certificados_31_Agosto_2018[column] = df_certificados_31_Agosto_2018[column].str.replace("'", "")
    df_certificados_31_Agosto_2018['RUC'] = df_certificados_31_Agosto_2018['RUC'].str.replace('.0', '')
    df_certificados_31_Agosto_2018['RUC'] = df_certificados_31_Agosto_2018['RUC'].str.replace('-', '')
    df_certificados_31_Agosto_2018.info()
    print('Proceso de ETL Finalizado')
    return df_certificados_31_Agosto_2018


def certificadosDiciembre2019():
    # Leer y limpiar data del excel CERTIFICADOS TOTAL HASTA 31 de Diciembre 2019
    print('Iniciado el proceso de ETL de CERTIFICADOS TOTAL HASTA 31 de Diciembre 2019')
    path_certificados_31_Diciembre_2019 = r'E:\Inteligencia de Negocios\28. Reportería Gerencia\CERTIFICADOS TOTAL HASTA 31 Diciembre 2019.xlsx'
    df_certificados_31_Diciembre_2019 = pd.read_excel(path_certificados_31_Diciembre_2019, sheet_name='CERTIFICADOS',
                                                      skiprows=7)
    old_columns_name = ['Almacenamiento', 'No Años', 'Obervación']
    new_columns_name = ['MEDIO EMISION', 'No Anos', 'Observacion']
    delete_columns = ['No']
    columns_number_comilla = ['RUC', 'CEDULA No', 'Num Factura']
    df_certificados_31_Diciembre_2019.rename(columns=dict(zip(old_columns_name, new_columns_name)), inplace=True)
    df_certificados_31_Diciembre_2019.drop(delete_columns, axis=1, inplace=True)
    for column in columns_number_comilla:
        df_certificados_31_Diciembre_2019[column] = df_certificados_31_Diciembre_2019[column].str.replace("'", "")
    df_certificados_31_Diciembre_2019['RUC'] = df_certificados_31_Diciembre_2019['RUC'].str.replace('.0', '')
    df_certificados_31_Diciembre_2019['RUC'] = df_certificados_31_Diciembre_2019['RUC'].str.replace('-', '')
    df_certificados_31_Diciembre_2019.info()
    print('Proceso de ETL Finalizado')
    return df_certificados_31_Diciembre_2019


def certificadosOctubre2020():
    # Leer y limpiar data del excel CERTIFICADOS TOTAL HASTA 31 de Octubre 2020
    print('Iniciado el proceso de ETL de CERTIFICADOS TOTAL HASTA 31 OCTUBRE 2020')
    path_certificados_31_Octubre_2020 = r'E:\Inteligencia de Negocios\28. Reportería Gerencia\CERTIFICADOS TOTAL HASTA 31 OCTUBRE 2020.xlsx'
    df_certificados_31_Octubre_2020 = pd.read_excel(path_certificados_31_Octubre_2020, sheet_name='CERTIFICADOS',
                                                    skiprows=7)
    old_columns_name = ['PFX', 'No Años', 'Obervación']
    new_columns_name = ['MEDIO EMISION', 'No Anos', 'Observacion']
    delete_columns = ['No']
    columns_number_comilla = ['RUC', 'CEDULA No', 'Num Factura']
    df_certificados_31_Octubre_2020.rename(columns=dict(zip(old_columns_name, new_columns_name)), inplace=True)
    df_certificados_31_Octubre_2020.drop(delete_columns, axis=1, inplace=True)
    for column in columns_number_comilla:
        df_certificados_31_Octubre_2020[column] = df_certificados_31_Octubre_2020[column].str.replace("'", "")
    df_certificados_31_Octubre_2020['RUC'] = df_certificados_31_Octubre_2020['RUC'].str.replace('.0', '')
    df_certificados_31_Octubre_2020['RUC'] = df_certificados_31_Octubre_2020['RUC'].str.replace('-', '')
    df_certificados_31_Octubre_2020.info()
    print('Proceso de ETL Finalizado')
    return df_certificados_31_Octubre_2020


def certificadosJulio2021():
    # Leer y limpiar data del excel CERTIFICADOS TOTAL HASTA 31 de Julio 2021
    print('Iniciado el proceso de ETL de CERTIFICADOS TOTAL HASTA 31 de Julio 2021')
    path_certificados_31_Julio_2021 = r'E:\Inteligencia de Negocios\28. Reportería Gerencia\CERTIFICADOS TOTAL HASTA 31 JULIO 2021.xlsx'
    df_certificados_31_Julio_2021 = pd.read_excel(path_certificados_31_Julio_2021, sheet_name='CERTIFICADOS',
                                                  skiprows=7)
    old_columns_name = ['No Años', 'Obervación']
    new_columns_name = ['No Anos', 'Observacion']
    delete_columns = ['No']
    columns_number_comilla = ['RUC', 'CEDULA No', 'Num Factura']
    df_certificados_31_Julio_2021.rename(columns=dict(zip(old_columns_name, new_columns_name)), inplace=True)
    df_certificados_31_Julio_2021.drop(delete_columns, axis=1, inplace=True)
    for column in columns_number_comilla:
        df_certificados_31_Julio_2021[column] = df_certificados_31_Julio_2021[column].str.replace("'", "")
    df_certificados_31_Julio_2021['RUC'] = df_certificados_31_Julio_2021['RUC'].str.replace('.0', '')
    df_certificados_31_Julio_2021['RUC'] = df_certificados_31_Julio_2021['RUC'].str.replace('-', '')
    df_certificados_31_Julio_2021.info()
    print('Proceso de ETL Finalizado')
    return df_certificados_31_Julio_2021


def certificadosDiciembre2021():
    # Leer y limpiar data del excel CERTIFICADOS TOTAL HASTA 31 de Diciembre 2021
    print('Iniciado el proceso de ETL de CERTIFICADOS TOTAL HASTA 31 de Diciembre 2021')
    path_certificados_31_Diciembre_2021 = r'E:\Inteligencia de Negocios\28. Reportería Gerencia\CERTIFICADOS TOTAL HASTA 31 DICIEMBRE 2021.xlsx'
    df_certificados_31_Diciembre_2021 = pd.read_excel(path_certificados_31_Diciembre_2021, sheet_name='CERTIFICADOS',
                                                      skiprows=7)
    old_columns_name = ['No Años', 'Obervación']
    new_columns_name = ['No Anos', 'Observacion']
    delete_columns = ['No']
    columns_number_comilla = ['RUC', 'CEDULA No', 'Num Factura']
    df_certificados_31_Diciembre_2021.rename(columns=dict(zip(old_columns_name, new_columns_name)), inplace=True)
    df_certificados_31_Diciembre_2021.drop(delete_columns, axis=1, inplace=True)
    for column in columns_number_comilla:
        df_certificados_31_Diciembre_2021[column] = df_certificados_31_Diciembre_2021[column].str.replace("'", "")
    df_certificados_31_Diciembre_2021['RUC'] = df_certificados_31_Diciembre_2021['RUC'].str.replace('.0', '')
    df_certificados_31_Diciembre_2021['RUC'] = df_certificados_31_Diciembre_2021['RUC'].str.replace('-', '')
    df_certificados_31_Diciembre_2021.info()
    print('Proceso de ETL Finalizado')
    return df_certificados_31_Diciembre_2021


def certificadosJunio2022():
    # Leer y limpiar data del excel CERTIFICADOS TOTAL HASTA 30 de Junio 2022
    print('Iniciado el proceso de ETL de CERTIFICADOS TOTAL HASTA 30 de Junio 2022')
    path_certificados_30_Junio_2022 = r'E:\Inteligencia de Negocios\28. Reportería Gerencia\CERTIFICADOS TOTAL HASTA EL 30 JUNIO 2022.xlsx'
    df_certificados_30_Junio_2022 = pd.read_excel(path_certificados_30_Junio_2022, sheet_name='CERTIFICADOS',
                                                  skiprows=7)
    old_columns_name = ['No Años', 'Obervación']
    new_columns_name = ['No Anos', 'Observacion']
    delete_columns = ['No']
    columns_number_comilla = ['RUC', 'CEDULA No', 'Num Factura']
    df_certificados_30_Junio_2022.rename(columns=dict(zip(old_columns_name, new_columns_name)), inplace=True)
    df_certificados_30_Junio_2022.drop(delete_columns, axis=1, inplace=True)
    for column in columns_number_comilla:
        df_certificados_30_Junio_2022[column] = df_certificados_30_Junio_2022[column].str.replace("'", "")
    df_certificados_30_Junio_2022['RUC'] = df_certificados_30_Junio_2022['RUC'].str.replace('.0', '')
    df_certificados_30_Junio_2022['RUC'] = df_certificados_30_Junio_2022['RUC'].str.replace('-', '')
    df_certificados_30_Junio_2022.info()
    print('Proceso de ETL Finalizado')
    return df_certificados_30_Junio_2022


def certificadosOctubre2022():
    # Leer y limpiar data del excel CERTIFICADOS TOTAL HASTA 31 de Octubre 2022
    print('Iniciado el proceso de ETL de CERTIFICADOS TOTAL HASTA 31 de Octubre 2022')
    path_certificados_31_Octubre_2022 = r'E:\Inteligencia de Negocios\28. Reportería Gerencia\CERTIFICADOS TOTAL HASTA EL 31 DE OCTUBRE 2022.xlsx'
    df_certificados_31_Octubre_2022 = pd.read_excel(path_certificados_31_Octubre_2022, sheet_name='CERTIFICADOS',
                                                    skiprows=7)
    old_columns_name = ['No Años', 'Obervación', 'CN en el sistema']
    new_columns_name = ['No Anos', 'Observacion', 'CN en el Sistema']
    delete_columns = ['No']
    columns_number_comilla = ['RUC', 'CEDULA No', 'Num Factura']
    df_certificados_31_Octubre_2022.rename(columns=dict(zip(old_columns_name, new_columns_name)), inplace=True)
    df_certificados_31_Octubre_2022.drop(delete_columns, axis=1, inplace=True)
    for column in columns_number_comilla:
        df_certificados_31_Octubre_2022[column] = df_certificados_31_Octubre_2022[column].str.replace("'", "")
    df_certificados_31_Octubre_2022['RUC'] = df_certificados_31_Octubre_2022['RUC'].str.replace('.0', '')
    df_certificados_31_Octubre_2022['RUC'] = df_certificados_31_Octubre_2022['RUC'].str.replace('-', '')
    df_certificados_31_Octubre_2022.info()
    print('Proceso de ETL Finalizado')
    return df_certificados_31_Octubre_2022


def certificadosDiciembre2022():
    # Leer y limpiar data del excel CERTIFICADOS TOTAL HASTA 31 de Diciembre 2022
    print('Iniciado el proceso de ETL de CERTIFICADOS TOTAL HASTA 31 de Diciembre 2022')
    path_certificados_31_Diciembre_2022 = r'E:\Inteligencia de Negocios\28. Reportería Gerencia\CERTIFICADOS TOTAL HASTA EL 31 DICIEMBRE 2022.xlsx'
    df_certificados_31_Diciembre_2022 = pd.read_excel(path_certificados_31_Diciembre_2022, sheet_name='CERTIFICADOS',
                                                      skiprows=7)
    old_columns_name = ['No Años', 'Obervación', 'CN en el sistema']
    new_columns_name = ['No Anos', 'Observacion', 'CN en el Sistema']
    delete_columns = ['No']
    columns_number_comilla = ['RUC', 'CEDULA No', 'Num Factura']
    df_certificados_31_Diciembre_2022.rename(columns=dict(zip(old_columns_name, new_columns_name)), inplace=True)
    df_certificados_31_Diciembre_2022.drop(delete_columns, axis=1, inplace=True)
    for column in columns_number_comilla:
        df_certificados_31_Diciembre_2022[column] = df_certificados_31_Diciembre_2022[column].str.replace("'", "")
    df_certificados_31_Diciembre_2022['RUC'] = df_certificados_31_Diciembre_2022['RUC'].str.replace('.0', '')
    df_certificados_31_Diciembre_2022['RUC'] = df_certificados_31_Diciembre_2022['RUC'].str.replace('-', '')
    df_certificados_31_Diciembre_2022.info()
    print('Proceso de ETL Finalizado')
    return df_certificados_31_Diciembre_2022


def certificadosAbril2023():
    # Leer y limpiar data del excel CERTIFICADOS TOTAL HASTA 30 de Abril 2023
    print('Iniciado el proceso de ETL de CERTIFICADOS TOTAL HASTA 30 de Abril 2023')
    path_certificados_30_Abril_2023 = r'E:\Inteligencia de Negocios\28. Reportería Gerencia\CERTIFICADOS NUEVO - HASTA EL 30 ABRIL 2023.xlsx'
    df_certificados_30_Abril_2023 = pd.read_excel(path_certificados_30_Abril_2023, sheet_name='CERTIFICADOS',
                                                  skiprows=7)
    old_columns_name = ['No Años', 'Obervación', 'CN en el sistema']
    new_columns_name = ['No Anos', 'Observacion', 'CN en el Sistema']
    delete_columns = ['No']
    columns_number_comilla = ['RUC', 'CEDULA No', 'Num Factura']
    df_certificados_30_Abril_2023.rename(columns=dict(zip(old_columns_name, new_columns_name)), inplace=True)
    df_certificados_30_Abril_2023.drop(delete_columns, axis=1, inplace=True)
    for column in columns_number_comilla:
        df_certificados_30_Abril_2023[column] = df_certificados_30_Abril_2023[column].str.replace("'", "")
    df_certificados_30_Abril_2023['RUC'] = df_certificados_30_Abril_2023['RUC'].str.replace('.0', '')
    df_certificados_30_Abril_2023['RUC'] = df_certificados_30_Abril_2023['RUC'].str.replace('-', '')
    df_certificados_30_Abril_2023.info()
    print('Proceso de ETL Finalizado')
    return df_certificados_30_Abril_2023


def certificadosSeptiembre2023():
    # Leer y limpiar data del excel CERTIFICADOS TOTAL HASTA 30 de Septiembre 2023
    print('Iniciado el proceso de ETL de CERTIFICADOS TOTAL HASTA 30 de Septiembre 2023')
    path_certificados_30_Septiembre_2023 = r'E:\Inteligencia de Negocios\28. Reportería Gerencia\CERTIFICADOS TOTAL HASTA EL 30 SEPTIEMBRE 2023.xlsx'
    df_certificados_30_Septiembre_2023 = pd.read_excel(path_certificados_30_Septiembre_2023, sheet_name='CERTIFICADOS',
                                                       skiprows=7)
    old_columns_name = ['No Años', 'Obervación', 'CN en el sistema']
    new_columns_name = ['No Anos', 'Observacion', 'CN en el Sistema']
    delete_columns = ['No']
    columns_number_comilla = ['RUC', 'CEDULA No', 'Num Factura']
    df_certificados_30_Septiembre_2023.rename(columns=dict(zip(old_columns_name, new_columns_name)), inplace=True)
    df_certificados_30_Septiembre_2023.drop(delete_columns, axis=1, inplace=True)
    for column in columns_number_comilla:
        df_certificados_30_Septiembre_2023[column] = df_certificados_30_Septiembre_2023[column].astype(str).str.replace("'", "")
    df_certificados_30_Septiembre_2023['RUC'] = df_certificados_30_Septiembre_2023['RUC'].str.replace('.0', '')
    df_certificados_30_Septiembre_2023['RUC'] = df_certificados_30_Septiembre_2023['RUC'].str.replace('-', '')
    df_certificados_30_Septiembre_2023.info()
    print('Proceso de ETL Finalizado')
    return df_certificados_30_Septiembre_2023


def certificadosDiciembre2023():
    # Leer y limpiar data del excel CERTIFICADOS TOTAL HASTA 31 de Diciembre 2023
    print('Iniciado el proceso de ETL de CERTIFICADOS TOTAL HASTA 31 de Diciembre 2023')
    path_certificados_31_Diciembre_2023 = r'E:\Inteligencia de Negocios\28. Reportería Gerencia\CERTIFICADOS TOTAL HASTA EL 31 DICIEMBRE 2023.xlsx'
    df_certificados_31_Diciembre_2023 = pd.read_excel(path_certificados_31_Diciembre_2023, sheet_name='CERTIFICADOS',
                                                      skiprows=7)
    old_columns_name = ['No Años', 'Obervación', 'CN en el sistema']
    new_columns_name = ['No Anos', 'Observacion', 'CN en el Sistema']
    delete_columns = ['No']
    columns_number_comilla = ['RUC', 'CEDULA No', 'Num Factura']
    df_certificados_31_Diciembre_2023.rename(columns=dict(zip(old_columns_name, new_columns_name)), inplace=True)
    df_certificados_31_Diciembre_2023.drop(delete_columns, axis=1, inplace=True)
    for column in columns_number_comilla:
        df_certificados_31_Diciembre_2023[column] = df_certificados_31_Diciembre_2023[column].astype(str).str.replace("'", "")
    df_certificados_31_Diciembre_2023['RUC'] = df_certificados_31_Diciembre_2023['RUC'].str.replace('.0', '')
    df_certificados_31_Diciembre_2023['RUC'] = df_certificados_31_Diciembre_2023['RUC'].str.replace('-', '')
    df_certificados_31_Diciembre_2023.info()
    print('Proceso de ETL Finalizado')
    return df_certificados_31_Diciembre_2023


def certificadosTokens():
    # Leer y limpiar data del excel CERTIFICADOS Tokens
    print('Iniciado el proceso de ETL de CERTIFICADOS Tokens')
    path_certificados_Tokens = r'E:\Inteligencia de Negocios\28. Reportería Gerencia\FIRMAS TOKEN.xlsx'
    df_certificados_Tokens = pd.read_excel(path_certificados_Tokens, sheet_name='Hoja1')
    old_columns_name = ['No Años', 'Obervación', 'CN en el sistema']
    new_columns_name = ['No Anos', 'Observacion', 'CN en el Sistema']
    delete_columns = ['No']
    columns_number_comilla = ['RUC', 'CEDULA No', 'Num Factura']
    df_certificados_Tokens.rename(columns=dict(zip(old_columns_name, new_columns_name)), inplace=True)
    df_certificados_Tokens.drop(delete_columns, axis=1, inplace=True)
    for column in columns_number_comilla:
        df_certificados_Tokens[column] = df_certificados_Tokens[column].astype(str).str.replace("'", "")
    df_certificados_Tokens['RUC'] = df_certificados_Tokens['RUC'].str.replace('.0', '')
    df_certificados_Tokens['RUC'] = df_certificados_Tokens['RUC'].str.replace('-', '')
    df_certificados_Tokens.info()
    print('Proceso de ETL Finalizado')
    return df_certificados_Tokens


def main():

    df_certificados_31_Agosto_2018 = certificadosAgosto2018()
    cargar_archivo_en_batches(dataframe_excel=df_certificados_31_Agosto_2018,
                              tabla_destino='certificados',
                              batch_size=1000)
    df_certificados_31_Diciembre_2019 = certificadosDiciembre2019()
    cargar_archivo_en_batches(dataframe_excel=df_certificados_31_Diciembre_2019,
                              tabla_destino='certificados',
                              batch_size=1000)
    df_certificados_31_Octubre_2020 = certificadosOctubre2020()
    cargar_archivo_en_batches(dataframe_excel=df_certificados_31_Octubre_2020,
                              tabla_destino='certificados',
                              batch_size=1000)
    df_certificados_31_Julio_2021 = certificadosJulio2021()
    cargar_archivo_en_batches(dataframe_excel=df_certificados_31_Julio_2021,
                              tabla_destino='certificados',
                              batch_size=1000)
    df_certificados_31_Diciembre_2021 = certificadosDiciembre2021()
    cargar_archivo_en_batches(dataframe_excel=df_certificados_31_Diciembre_2021,
                              tabla_destino='certificados',
                              batch_size=1000)
    df_certificados_30_Junio_2022 = certificadosJunio2022()
    cargar_archivo_en_batches(dataframe_excel=df_certificados_30_Junio_2022,
                              tabla_destino='certificados',
                              batch_size=1000)

    df_certificados_31_Octubre_2022 = certificadosOctubre2022()
    cargar_archivo_en_batches(dataframe_excel=df_certificados_31_Octubre_2022,
                              tabla_destino='certificados',
                              batch_size=1000)
    df_certificados_31_Diciembre_2022 = certificadosDiciembre2022()
    cargar_archivo_en_batches(dataframe_excel=df_certificados_31_Diciembre_2022,
                              tabla_destino='certificados',
                              batch_size=1000)
    df_certificados_30_Abril_2023 = certificadosAbril2023()
    cargar_archivo_en_batches(dataframe_excel=df_certificados_30_Abril_2023,
                              tabla_destino='certificados',
                              batch_size=1000)
    df_certificados_30_Septiembre_2023 = certificadosSeptiembre2023()
    cargar_archivo_en_batches(dataframe_excel=df_certificados_30_Septiembre_2023,
                              tabla_destino='certificados',
                              batch_size=1000)
    df_certificados_31_Diciembre_2023 = certificadosDiciembre2023()
    cargar_archivo_en_batches(dataframe_excel=df_certificados_31_Diciembre_2023,
                              tabla_destino='certificados',
                              batch_size=1000)

    df_certificados_Tokens = certificadosTokens()
    cargar_archivo_en_batches(dataframe_excel=df_certificados_Tokens,
                              tabla_destino='certificados',
                              batch_size=1000)


if __name__ == '__main__':
    main()
