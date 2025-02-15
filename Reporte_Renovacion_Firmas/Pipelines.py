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
    query_path = r'C:\Users\rtoledo\PycharmProjects\ETL_BI\Reporte_Renovacion_Firmas\Consultas SQL\ConsultaCamunda.sql'
    # Se considera unicamente Emision y Renovacion de Firma electronica, no Los Sist. de Facturacion




    # Create a pipeline
    firmas_camunda_pipeline = Pipeline([
        ('extract_data', dbm.DataExtractor(database='CAMUNDA', query_path=query_path)),
        ('delete_duplicates', fgp.DeleteDuplicateEntries(column='id_tramite')),
        ('Filtrar_Suspendidas_Revocadas', fgp.FilterPerNotListMatchs(match_column='estado_firma', match_list=['Revocado', 'Suspendido'])),
        ('Establecer Firmas Caducadas', fgp.DeterminarFirmasCaducadas('fecha_caducidad')),
        ('Replace_Guion_Nulls_RUC', fgp.ReplaceValues('ruc', ['-'], [np.nan])),
        ('Replace_Blank_Nulls_RUC', fgp.ReplaceValues('ruc', [''], [np.nan])),
        ('Create_Column_key', fgp.CrearKeyWithCedulaRucTP('cedula', 'ruc', 'tipofirma', 'key')),
    ])

    # Ejecutar el pipeline de ETL
    extracted_data = firmas_camunda_pipeline.fit_transform(None)
    return extracted_data


# Firmas Aprobdas del POrtal SUBCA cuya feha de caducidad es mayor a '2024-06-01'
def Firmas_Portal():
    # Query to extract data from the CAMUNDA database
    # Consulta SQL
    query_path = r'C:\Users\rtoledo\PycharmProjects\ETL_BI\Reporte_Renovacion_Firmas\Consultas SQL\ConsultaPortalSQL.sql'
    # Se consideran unicametne las firmas vigentes, las firmas suspendidas y revocadas no son consideradas para este
    # analisis, puesto que se requiere unicamente las firmas que estan caducandose en un periodo de 6 meses


    # Create a pipeline
    firmas_portal_pipeline = Pipeline([
        ('extract_data', dbmMySQL.DataExtractor(database='Portal', query_path=query_path)),
        ('Filter_Per_Date', fgp.FilterPerDate(date_column='fecha_caducidad', start_date='2024-01-01')),
        ('Replace_values_Renovacion', fgp.ReplaceValues('producto', ['No', 'Si'], ['Emisión', 'Renovación'])),
        ('Replace_values_estado', fgp.ReplaceValues('estado_firma', [5, 1], ['Aprobado', 'Emitido'])),
        ('Replace_values_formapago', fgp.ReplaceValues('forma_pago', ["20", "16", "19"],
                                                       ['Transferencia/Deposito', 'Pago en Linea', "Pago en Linea"])),
        ('Establecer Firmas Caducadas', fgp.DeterminarFirmasCaducadas('fecha_caducidad')),
        ('Agregar_columna', fgp.AgregarColumnaValor(column_name='estado_tramite', value='Finalización de Trámite')),
        ('Replace_Blank_Nulls_RUC', fgp.ReplaceValues('ruc', [''], [np.nan])),
        ('Create_Column_key', fgp.CrearKeyWithCedulaRucTP('cedula', 'ruc', 'tipofirma', 'key')),
    ])

    # Ejecutar el pipeline de ETL
    extracted_data = firmas_portal_pipeline.fit_transform(None)
    return extracted_data


def Biometria():
    query_path = r'C:\Users\rtoledo\PycharmProjects\ETL_BI\Reporte_Renovacion_Firmas\Consultas SQL\Consulta_Biometrias.txt'

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


def DataBaseConcatenationPortalCamunda():
    df_portal = Firmas_Portal()
    df_camunda = FirmasCamunda()


    concatenated_pipelines = Pipeline([
        ('concatenate', fgp.ConcatenatedDataFrames(df1=df_camunda, df2=df_portal, axis=0)),
        ('Filtrar_medio_cam_archivo_only', fgp.FilterPerListMatchs(match_column='mediocam', match_list=['Archivo'])),
        ('delete_duplicated_serialfirma', fgp.DeleteDuplicateEntries(column='serial_firma')),
        ('Identificar Maximo Fecha Caducidad', fgp.IdentificarMaxDateGroups('fecha_caducidad', 'key')),
        #('Identificar Minimo Fecha Init Tramite', fgp.IdentificarMinDateGroups('fecha_inicio_tramite', 'key')),
        ('Transform_FechaInicioTramite_DateTime', fgp.DTypeDateTime(columns=['fecha_caducidad'])),
        ('Indicar pediodo de Renovacion', fgp.VerificarPeriodoRenovacion('fecha_inicio_tramite', 'fecha_caducidad', 'key')),
        ('Lipieza Correos Preferenciales', fgp.LimpiezaCorreosPref('correos')),
    ])

    df_portal_camunda = concatenated_pipelines.fit_transform(None)
    return df_portal_camunda


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
    query_path = r'C:\Users\rtoledo\PycharmProjects\ETL_BI\Reporte_Renovacion_Firmas\Consultas SQL\Solicitudes_Proceso_Camunda.sql'
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
    df = DataBaseConcatenationPortalCamunda()
    df.info()


if __name__ == '__main__':
    main()
