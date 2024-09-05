import os

import GeneralFunctions.FunctionGeneralPurpose as fgp
import GeneralFunctions.DataBaseManagement as dbm
import GeneralFunctions.DataBaseManagementSQL as dbmMySQL
import numpy as np
import pandas as pd
from sklearn.pipeline import Pipeline


# Firmas aprobadas en Archivo del Camunda cuya fecha de caducidad es mayor a '2024-06-01'
def FirmasCamunda():
    # Query to extract data from the CAMUNDA database
    # Consulta SQL
    # 482 Renovacion Firma Electronica
    # 481 Emision Firma Electronica
    # 487 Emision SF + Firma
    # 489 Renovacion SF + Firma
    query_path = r"C:\Users\cbenalcazar\PycharmProjects\BussinesIntelligenceFunctions\Reporte_Renovacion_Firmas\Consultas SQL\ConsultaCamunda.sql"
    # Se considera unicamente Emision y Renovacion de Firma electronica, no Los Sist. de Facturacion




    # Create a pipeline
    firmas_camunda_pipeline = Pipeline([
        #('extract_data', dbm.DataExtractor(database='CAMUNDA', query_path=query_path)),
        ('Cargar el CSV', fgp.CSVLoaderTransformer(
            r'C:\Users\cbenalcazar\PycharmProjects\BussinesIntelligenceFunctions\DatabasesConsultas\FirmasCamunda.csv')),
        ('Convertir ID a Object', fgp.DTypeObject(column=['id_tramite'])),
        ('delete_duplicates', fgp.DeleteDuplicateEntries(column='id_tramite')),
        ('Filtrar_Suspendidas_Revocadas', fgp.FilterPerNotListMatchs(match_column='estado_firma', match_list=['Revocado', 'Suspendido'])),
        ('Establecer Firmas Caducadas', fgp.DeterminarFirmasCaducadas('fecha_caducidad')),
        ('Replace_Guion_Nulls_RUC', fgp.ReplaceValues('ruc', ['-'], [np.nan])),
        ('Replace_Blank_Nulls_RUC', fgp.ReplaceValues('ruc', [''], [np.nan])),
        ('Create_Column_key', fgp.CrearKeyWithCedulaRucTP('cedula', 'ruc', 'tipofirma', 'key')),
        ('Actualizacion de Date Init por Date Fact', fgp.ChangeDateInitTramFact('fecha_inicio_tramite', 'fecha_factura')),
    ])

    # Ejecutar el pipeline de ETL
    extracted_data = firmas_camunda_pipeline.fit_transform(None)
    return extracted_data


# Firmas Aprobdas del POrtal SUBCA cuya feha de caducidad es mayor a '2024-06-01'
def Firmas_Portal():
    # Query to extract data from the CAMUNDA database
    # Consulta SQL
    query_path = r"C:\Users\cbenalcazar\PycharmProjects\BussinesIntelligenceFunctions\Reporte_Renovacion_Firmas\Consultas SQL\ConsultaPortalSQL.sql"
    # Se consideran unicametne las firmas vigentes, las firmas suspendidas y revocadas no son consideradas para este
    # analisis, puesto que se requiere unicamente las firmas que estan caducandose en un periodo de 6 meses


    # Create a pipeline
    firmas_portal_pipeline = Pipeline([
        #('extract_data', dbmMySQL.DataExtractor(database='Portal', query_path=query_path)),
        ('Cargar el CSV', fgp.CSVLoaderTransformer(r'C:\Users\cbenalcazar\PycharmProjects\BussinesIntelligenceFunctions\DatabasesConsultas\FirmasPortal.csv')),
        ('Filter_Per_Date', fgp.FilterPerDate(date_column='fecha_caducidad', start_date='2021-01-01')),
        ('Replace_values_Renovacion', fgp.ReplaceValues('producto', ['No', 'Si'], ['Emisión', 'Renovación'])),
        ('Replace_values_estado', fgp.ReplaceValues('estado_firma', [5, 1], ['Aprobado', 'Emitido'])),
        ('Replace_values_formapago', fgp.ReplaceValues('forma_pago', ["20", "16", "19"],
                                                       ['Transferencia/Deposito', 'Pago en Linea', "Pago en Linea"])),
        ('Establecer Firmas Caducadas', fgp.DeterminarFirmasCaducadas('fecha_caducidad')),
        ('Agregar_columna', fgp.AgregarColumnaValor(column_name='estado_tramite', value='Finalización de Trámite')),
        ('Replace_Blank_Nulls_RUC', fgp.ReplaceValues('ruc', [''], [np.nan])),
        ('Create_Column_key', fgp.CrearKeyWithCedulaRucTP('cedula', 'ruc', 'tipofirma', 'key')),
        ('Actualizacion de Date Init por Date Fact', fgp.ChangeDateInitTramFact('fecha_inicio_tramite', 'fecha_fact')),
    ])

    # Ejecutar el pipeline de ETL
    extracted_data = firmas_portal_pipeline.fit_transform(None)
    return extracted_data


def Biometria():
    query_path = r"C:\Users\cbenalcazar\PycharmProjects\BussinesIntelligenceFunctions\Reporte_Renovacion_Firmas\Consultas SQL\Consulta_Biometrias.txt"

    # 482 Renovacion Firma Electronica
    # 481 Emision Firma Electronica
    # 487 Emision SF + Firma
    # 489 Renovacion SF + Firma

    biometrias_pipeline = Pipeline([
        ('extract_data', dbm.DataExtractor(database='CAMUNDA', query_path=query_path)),
        ('boolean_2_String', fgp.BooleanToString('biometria_aprobada')),
        ('delete_duplicates', fgp.DeleteDuplicateEntries(column='id_tramite'))
    ])
    # Ejecutar el pipeline de ETL
    biometrias_data = biometrias_pipeline.fit_transform(None)
    return biometrias_data


def Firmas_Tokens():
    # Query to extract data from the CAMUNDA database
    # Consulta SQL
    query_path = r"C:\Users\cbenalcazar\PycharmProjects\BussinesIntelligenceFunctions\Reporte_Renovacion_Firmas\Consultas SQL\ConsultaPortalSQL_Tokens.sql"
    # Se consideran unicametne las firmas vigentes, las firmas suspendidas y revocadas no son consideradas para este
    # analisis, puesto que se requiere unicamente las firmas que estan caducandose en un periodo de 6 meses


    # Create a pipeline
    firmas_portal_pipeline = Pipeline([
        #('extract_data', dbmMySQL.DataExtractor(database='Portal', query_path=query_path)),
        ('Cargar el CSV', fgp.CSVLoaderTransformer(r'C:\Users\cbenalcazar\PycharmProjects\BussinesIntelligenceFunctions\DatabasesConsultas\TokensPortal.csv')),
        ('Filter_Per_Date', fgp.FilterPerDate(date_column='fecha_caducidad', start_date='2021-01-01')),
        ('Replace_values_Renovacion', fgp.ReplaceValues('producto', ['No', 'Si'], ['Emisión', 'Renovación'])),
        ('Replace_values_estado', fgp.ReplaceValues('estado_firma', [5, 1], ['Aprobado', 'Emitido'])),
        ('Replace_values_formapago', fgp.ReplaceValues('forma_pago', ["20", "16", "19"],
                                                       ['Transferencia/Deposito', 'Pago en Linea', "Pago en Linea"])),
        ('Establecer Firmas Caducadas', fgp.DeterminarFirmasCaducadas('fecha_caducidad')),
        ('Agregar_columna', fgp.AgregarColumnaValor(column_name='estado_tramite', value='Finalización de Trámite')),
        ('Replace_Blank_Nulls_RUC', fgp.ReplaceValues('ruc', [''], [np.nan])),
        ('Create_Column_key', fgp.CrearKeyWithCedulaRucTP('cedula', 'ruc', 'tipofirma', 'key')),
        ('Actualizacion de Date Init por Date Fact', fgp.ChangeDateInitTramFact('fecha_inicio_tramite', 'fecha_fact')),
    ])
    # Ejecutar el pipeline de ETL
    extracted_data = firmas_portal_pipeline.fit_transform(None)
    return extracted_data



# Firmas aprobadas en Archivo del Camunda cuya fecha de caducidad es mayor a '2024-06-01'
def FirmasCamundaTokens():
    # Query to extract data from the CAMUNDA database
    # Consulta SQL
    # 482 Renovacion Firma Electronica
    # 481 Emision Firma Electronica
    # 487 Emision SF + Firma
    # 489 Renovacion SF + Firma
    query_path = r"C:\Users\cbenalcazar\PycharmProjects\BussinesIntelligenceFunctions\Reporte_Renovacion_Firmas\Consultas SQL\ConsultaCamunda_Tokens.sql"
    # Se considera unicamente Emision y Renovacion de Firma electronica, no Los Sist. de Facturacion

    # Create a pipeline
    firmas_camunda_pipeline = Pipeline([
        #('extract_data', dbm.DataExtractor(database='CAMUNDA', query_path=query_path)),
        ('Cargar el CSV', fgp.CSVLoaderTransformer(r'C:\Users\cbenalcazar\PycharmProjects\BussinesIntelligenceFunctions\DatabasesConsultas\TokensCamunda.csv')),
        ('Convertir ID a Object', fgp.DTypeObject(column=['id_tramite'])),
        ('delete_duplicates', fgp.DeleteDuplicateEntries(column='id_tramite')),
        ('Filtrar_Suspendidas_Revocadas', fgp.FilterPerNotListMatchs(match_column='estado_firma', match_list=['Revocado', 'Suspendido'])),
        ('Establecer Firmas Caducadas', fgp.DeterminarFirmasCaducadas('fecha_caducidad')),
        ('Replace_Guion_Nulls_RUC', fgp.ReplaceValues('ruc', ['-'], [np.nan])),
        ('Replace_Blank_Nulls_RUC', fgp.ReplaceValues('ruc', [''], [np.nan])),
        ('Create_Column_key', fgp.CrearKeyWithCedulaRucTP('cedula', 'ruc', 'tipofirma', 'key')),
        ('Actualizacion de Date Init por Date Fact', fgp.ChangeDateInitTramFact('fecha_inicio_tramite', 'fecha_factura')),
    ])

    # Ejecutar el pipeline de ETL
    extracted_data = firmas_camunda_pipeline.fit_transform(None)
    return extracted_data


def DataBaseConcatenationPortalCamunda():
    df_portal = Firmas_Portal()
    df_camunda = FirmasCamunda()


    concatenated_pipelines = Pipeline([
        ('concatenate', fgp.ConcatenatedDataFrames(df1=df_camunda, df2=df_portal, axis=0)),
        ('Filtrar_medio_cam_archivo_only', fgp.FilterPerListMatchs(match_column='mediocam', match_list=['Archivo'])),
        ('Lipieza Correos Preferenciales', fgp.LimpiezaCorreosPref('correo', 'key')),
        #('Filtrar_origen_proceso', fgp.FilterPerListMatchs(match_column='origen_proceso', match_list=['Security Data'])),
        ('delete_duplicated_serialfirma', fgp.DeleteDuplicateEntries(column='serial_firma')),
        ('Identificar Maximo Fecha Caducidad', fgp.IdentificarMaxDateGroups('fecha_caducidad', 'key')),
        #('Identificar Minimo Fecha Init Tramite', fgp.IdentificarMinDateGroups('fecha_inicio_tramite', 'key')),
        ('Transform_FechaInicioTramite_DateTime', fgp.DTypeDateTime(columns=['fecha_caducidad',
                                                                             'fecha_inicio_tramite',
                                                                             'fecha_fin_tramite',
                                                                             'fecha_aprobacion',
                                                                             'fecha_factura',
                                                                             'fecha_expedicion',
                                                                             'fecha_fact'])),
        ('Indicar pediodo de Renovacion', fgp.VerificarPeriodoRenovacion('fecha_inicio_tramite', 'fecha_caducidad', 'key', 'producto', 'vigencia')),
    ])

    df_portal_camunda = concatenated_pipelines.fit_transform(None)
    return df_portal_camunda


def DataBaseConcatenationPortalCamundaTokens():
    df_portal = Firmas_Tokens()
    df_camunda = FirmasCamundaTokens()

    concatenated_pipelines = Pipeline([
        ('concatenate', fgp.ConcatenatedDataFrames(df1=df_camunda, df2=df_portal, axis=0)),
        ('Limpieza Correos Preferenciales', fgp.LimpiezaCorreosPref('correo', 'key')),
        #('Filtrar_origen_proceso', fgp.FilterPerListMatchs(match_column='origen_proceso', match_list=['Security Data'])),
        ('delete_duplicated_serialfirma', fgp.DeleteDuplicateEntries(column='serial_firma')),
        ('Identificar Maximo Fecha Caducidad', fgp.IdentificarMaxDateGroups('fecha_caducidad', 'key')),
        #('Identificar Minimo Fecha Init Tramite', fgp.IdentificarMinDateGroups('fecha_inicio_tramite', 'key')),
        ('Transform_FechaInicioTramite_DateTime', fgp.DTypeDateTime(columns=['fecha_caducidad',
                                                                             'fecha_inicio_tramite',
                                                                             'fecha_fin_tramite',
                                                                             'fecha_aprobacion',
                                                                             'fecha_factura',
                                                                             'fecha_expedicion',
                                                                             'fecha_fact'])),
        ('Indicar pediodo de Renovacion', fgp.VerificarPeriodoRenovacion('fecha_inicio_tramite', 'fecha_caducidad', 'key', 'producto', 'vigencia')),
        ('delete_duplicated_idtramite', fgp.DeleteDuplicateEntries(column='id_tramite')),
    ])

    df_portal_camunda = concatenated_pipelines.fit_transform(None)
    return df_portal_camunda


def DataBaseConcatenationFirmasTokens():
    df_firmas = DataBaseConcatenationPortalCamundaTokens()
    df_tokens = DataBaseConcatenationPortalCamunda()

    concatenated_pipelines = Pipeline([
        ('concatenate', fgp.ConcatenatedDataFrames(df1=df_firmas, df2=df_tokens, axis=0)),
        ('delete_duplicated_serialfirma', fgp.DeleteDuplicateEntries(column='id_tramite')),
        ('Convertir Valor Factura a float', fgp.DTypeFloat(['subtotal', 'valorfactura'])),
        ('Corregir Valor a comisionar', fgp.ValueToComisionar('valorfactura', 'vigencia', 'valor_factura_comision', 'producto')),
        ('ContadorFirmasComisionar',
         fgp.ExtractNumerateRows('Mes de Renovacion', 'producto', 'vigencia', 'Mom. de renovacion', 'valor_factura_comision')),
    ])
    df_portal_camunda_firmas_tokens = concatenated_pipelines.fit_transform(None)
    return df_portal_camunda_firmas_tokens


def GetReportC0misiones(X):

    comisiones_pipelines = Pipeline([
        ('Get Reporte Comisiones', fgp.ReportComisiones()),
    ])
    df_reporte_comisiones = comisiones_pipelines.fit_transform(X)
    return df_reporte_comisiones

# Procesos Pendientes del Portal CAMUNDA
def ProcesosPendientesCamunda():
    # Query to extract data from the CAMUNDA database
    # Consulta SQL
    # 482 Renovacion Firma Electronica
    # 481 Emision Firma Electronica
    # 487 Emision SF + Firma
    # 489 Renovacion SF + Firma
    # 491 Recuperacion de Clave
    # 495 Agregar Ruc a la Firma
    query_path = r"C:\Users\cbenalcazar\PycharmProjects\BussinesIntelligenceFunctions\Reporte_Renovacion_Firmas\Consultas SQL\Solicitudes_Proceso_Camunda.sql"
    # Se considera unicamente Emision y Renovacion de Firma electronica, no Los Sist. de Facturacion


    # Create a pipeline
    procesos_pendientes_pipeline = Pipeline([
        ('extract_data', dbm.DataExtractor(database='CAMUNDA', query_path=query_path)),
        ('delete_duplicates', fgp.DeleteDuplicateEntries(column='id_tramite')),
        ('Filtrar_medio_cam_archivo_only', fgp.FilterPerListMatchs(match_column='mediocam', match_list=['Archivo'])),
        ('Replace_Guion_Nulls_RUC', fgp.ReplaceValues('ruc', ['-'], [np.nan])),
        ('Replace_Blank_Nulls_RUC', fgp.ReplaceValues('ruc', [''], [np.nan])),
        ('Create_Column_key', fgp.CrearKeyWithCedulaRucTP('cedula', 'ruc', 'tipofirma', 'key')),
    ])

    # Ejecutar el pipeline de ETL
    extracted_data = procesos_pendientes_pipeline.fit_transform(None)
    return extracted_data




def main():
    print("main")
    df = DataBaseConcatenationFirmasTokens()
    df.info()
    # Definir el path del archivo
    directory = r'C:/Users/cbenalcazar/PycharmProjects/BussinesIntelligenceFunctions/DatabasesConsultas'
    filename = f'Reporte Firmas General.csv'
    file_path = os.path.join(directory, filename)

    # Verificar si el directorio existe, y si no, crearlo
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Guardar el DataFrame como CSV
    df.to_csv(file_path, index=False)

    df_reporte_comisiones = GetReportC0misiones(df)
    # Definir el path del archivo
    directory = r'C:/Users/cbenalcazar/PycharmProjects/BussinesIntelligenceFunctions/DatabasesConsultas'
    filename = f'Reporte Comisiones.xlsx'
    file_path = os.path.join(directory, filename)

    # Verificar si el directorio existe, y si no, crearlo
    if not os.path.exists(directory):
        os.makedirs(directory)

    # Guardar el DataFrame como CSV
    df_reporte_comisiones.to_excel(file_path, index=False)


if __name__ == '__main__':
    main()
