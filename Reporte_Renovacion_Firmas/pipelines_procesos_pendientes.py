import os

import GeneralFunctions.FunctionGeneralPurpose as fgp
import GeneralFunctions.DataBaseManagement as dbm
import Reporte_Renovacion_Firmas.specialized_functons as sf
import GeneralFunctions.data_type_transformers as dtt

import numpy as np
import pandas as pd
from sklearn.pipeline import Pipeline


# Firmas aprobadas en Archivo del Camunda cuya fecha de caducidad es mayor a '2024-06-01'
def etl_camunda_database(query_path):
    # Query to extract data from the CAMUNDA database
    # Consulta SQL
    # Columna 1: t.id_tramite - Identificador del trámite - Tipo: int
    # Columna 2:  (u.nombre || ' ' || u.apellido_paterno || ' ' || u.apellido_materno) - Nombre completo del usuario (nombre + apellidos concatenados) - Tipo: string
    # Columna 3: d.correo - Correo electrónico del solicitante - Tipo: string
    # Columna 4: telefono - Teléfono principal del usuario - Tipo: string
    # Columna 5: flujo - Tipo de flujo relacionado con el trámite - Tipo: string
    # Columna 6: Ciudad - Nombre de la ciudad asociada - Tipo: string
    # Columna 7: t.id_user - Identificador del usuario - Tipo: int
    # Columna 8: cedula - Cédula del usuario o nombre de usuario - Tipo: string
    # Columna 9: ruc - Número RUC del solicitante o representante legal - Tipo: string
    # Columna 10: razon_social - Razón social de la empresa del representante legal - Tipo: string
    # Columna 11: forma_pago - Forma de pago utilizada - Tipo: string
    # Columna 12: descripcion - Descripción de la entidad de pago - Tipo: string
    # Columna 13: factura - Número de factura externa asociada - Tipo: string
    # Columna 14: fecha_inicio_tramite - Fecha de inicio del trámite - Tipo: date
    # Columna 15: fecha_fin_tramite - Fecha de fin del trámite - Tipo: date
    # Columna 16: fecha_aprobacion - Fecha de aprobación de la firma electrónica - Tipo: date
    # Columna 17: fecha_caducidad - Fecha de expiración de la firma electrónica - Tipo: date
    # Columna 18: serial_firma - Serial de la firma electrónica - Tipo: string
    # Columna 19: TipoFirma - Tipo de firma electrónica PN-ME-RL - Tipo: string
    # Columna 20: Atencion - Tipo de atención Si fue Online o Cita Express - Tipo: string
    # Columna 21: MedioCam - Tipo de contenedor de la firma si es archivo o Token - Tipo: string
    # Columna 22: estado_tramite - Estado actual del trámite - Tipo: string
    # Columna 23: estado_firma - Estado actual de la firma electrónica - Tipo: string
    # Columna 24: vigencia - Tiempo de vigencia de la firma electrónica (en años) - Tipo: int
    # Columna 25: fecha_factura - Fecha de emisión de la factura - Tipo: date
    # Columna 26: Producto - Producto que adquiere el comprador ya sea emision, renovacion, sf, etc - Tipo: string
    # Columna 27: medio_contacto - Medio de contacto del solicitante - Tipo: string
    # Columna 28: fs.correo - Correo relacionado con la firma electrónica - Tipo: string - Columna eliminada
    # Columna 29: subtotal - Subtotal de la factura - Tipo: float
    # Columna 30: valorfactura - Valor total de la factura - Tipo: float
    # Columna 31: MEMBER_of_operador - Nombre del operador que realizo el ingreso del proceso(Vendedor)- Tipo: string
    # Columna 32: agregar_token - Token asociado a la firma electrónica - Tipo: string
    # 482 Renovacion Firma Electronica
    # 481 Emision Firma Electronica
    # 487 Emision SF + Firma
    # 489 Renovacion SF + Firma

    # Se considera unicamente Emision y Renovacion de Firma electronica, no Los Sist. de Facturacion

    column_types = {
        'id_tramite': 'Int64',  # Identificador del trámite
        'Nombre': 'object',  # Nombre completo del usuario
        'correo': 'object',  # Correo electrónico del solicitante
        'telefono': 'object',  # Teléfono principal del usuario
        'flujo': 'category',  # Tipo de flujo relacionado con el trámite (categoría)
        'Ciudad': 'object',  # Nombre de la ciudad asociada
        'id_user': 'Int64',  # Identificador del usuario
        'cedula': 'object',  # Cédula del usuario o nombre de usuario
        'ruc': 'object',  # Número RUC del solicitante o representante legal
        'razon_social': 'object',  # Razón social de la empresa del representante legal
        'forma_pago': 'category',  # Forma de pago utilizada (categoría)
        'descripcion': 'object',  # Descripción de la entidad de pago
        'factura': 'object',  # Número de factura externa asociada
        'fecha_inicio_tramite': 'object',  # Fecha de inicio del trámite
        'fecha_fin_tramite': 'object',  # Fecha de fin del trámite
        'fecha_aprobacion': 'object',  # Fecha de aprobación de la firma electrónica
        'fecha_caducidad': 'object',  # Fecha de expiración de la firma electrónica
        'serial_firma': 'object',  # Serial de la firma electrónica
        'tipofirma': 'category',  # Tipo de firma electrónica (categoría)
        'atencion': 'category',  # Tipo de atención brindada (categoría)
        'mediocam': 'category',  # Medio de comunicación utilizado en el trámite (categoría)
        'estado_tramite': 'category',  # Estado actual del trámite (categoría)
        'estado_firma': 'category',  # Estado actual de la firma electrónica (categoría)
        'vigencia': 'category',  # Tiempo de vigencia de la firma electrónica (en años) (categoría)
        'fecha_factura': 'object',  # Fecha de emisión de la factura
        'producto': 'category',  # Producto relacionado al trámite (categoría)
        'medio_contacto': 'category',  # Medio de contacto del solicitante (categoría)
        'subtotal': 'float',  # Subtotal de la factura
        'valorfactura': 'float',  # Valor total de la factura
        'MEMBER_of_operador': 'object',  # Nombre del operador miembro del trámite
        'agregar_token': 'category'  # Token asociado a la firma electrónica
    }
    float_columns = ['subtotal', 'valorfactura']
    # Create a pipeline
    firmas_camunda_pipeline = Pipeline([
        ('extract_data',
         dbm.DataExtractorCamunda(database='CAMUNDA', query_path=query_path, dtypes_variables=column_types,
                                  save_request=True, float_columns=float_columns)),
        #('Cargar el CSV', fgp.CSVLoaderTransformer(r'C:\Users\cbenalcazar\PycharmProjects\BussinesIntelligenceFunctions\DatabasesConsultas\FirmasCamunda.csv')),
        ('Converte Date DateTime', fgp.DTypeDateTime([
            'fecha_inicio_tramite', 'fecha_fin_tramite', 'fecha_aprobacion',
            'fecha_caducidad', 'fecha_factura'
        ])),
        ('Count the amount of null Values', fgp.CountNullValuesRow('nulls_count')),
        ('delete_duplicated_serialfirma', fgp.DeleteDuplicateEntries(column='id_tramite', nulls_count='nulls_count')),
    ])

    # Ejecutar el pipeline de ETL
    extracted_data = firmas_camunda_pipeline.fit_transform(None)
    return extracted_data


def etl_portal_database(query_path):
    # Query to extract data from the CAMUNDA database
    # Firmas Aprobdas del POrtal SUBCA
    # Columna 1: cedula - Cédula del usuario - Tipo: object
    # Columna 2: vigencia - Tiempo de vigencia (años) - Tipo: object (puede incluir valores con 'M')
    # Columna 3: fecha_aprobacion - Fecha de aprobación - Tipo: datetime64
    # Columna 4: factura - Número de factura - Tipo: object
    # Columna 5: forma_pago - Forma de pago - Tipo: category
    # Columna 6: producto - Producto Adquirido como Renovacion, Emision, Emision SF, etc- Tipo: category
    # Columna 7: fecha_expedicion - Fecha de expedición - Tipo: datetime64
    # Columna 8: mediocam - Contenedor de la firma electronica - Tipo: category
    # Columna 9: fecha_caducidad_mod - Fecha de caducidad modificada(Sumada la vigencia la fecha de aprobacion) - Tipo: datetime64
    # Columna 10: razon_social - Razón social (vacío) - Tipo: object
    # Columna 11: ruc - RUC del usuario - Tipo: object
    # Columna 12: tipofirma - Tipo de firma RP-RL-ME - Tipo: object
    # Columna 13: atencion - Tipo de atención Cita Express, Atencion en Linea - Tipo: category
    # Columna 14: descripcion - Descripción del banco o entidad de pago - Tipo: object
    # Columna 15: fecha_factura - Fecha de emisión de la factura - Tipo: datetime64
    # Columna 16: estado_firma - Estado de la firma - Tipo: category
    # Columna 17: serial_firma - Serial de la firma electrónica - Tipo: object
    # Columna 18: fecha_inicio_tramite - Fecha de inicio del trámite - Tipo: datetime64
    # Columna 19: fecha_caducidad - Fecha de caducidad original - Tipo: datetime64
    # Columna 21: MEMBER_of_operador - Miembro del operador - Tipo: object
    # Columna 22: valorfactura - Valor total de la factura - Tipo: float
    # Columna 23: medio_contacto - Medio de contacto (vacío) - Tipo: object
    # Columna 24: Ciudad - Cantón o ciudad - Tipo: object
    # Columna 25: correo - Correo electrónico del usuario - Tipo: object
    # Columna 26: telefono - Teléfono del usuario - Tipo: object
    # Columna 27: Nombre - Nombre completo del usuario (nombre + apellidos) - Tipo: object
    # Columna 28: flujo - Flujo del trámite (vacío) - Tipo: object

    column_types = {
        'cedula': 'object',  # Cédula del usuario
        'vigencia': 'category',  # Tiempo de vigencia (puede incluir valores con 'M')
        'fecha_aprobacion': 'object',  # Fecha de aprobación
        'factura': 'object',  # Número de factura
        'forma_pago': 'category',  # Forma de pago
        'producto': 'category',  # Producto relacionado (renovación)
        'fecha_expedicion': 'object',  # Fecha de expedición
        'mediocam': 'category',  # Medio de comunicación
        'razon_social': 'object',  # Razón social (vacío)
        'ruc': 'object',  # RUC del usuario
        'tipofirma': 'category',  # Tipo de firma (fijo: 'PN')
        'atencion': 'category',  # Tipo de atención
        'descripcion': 'object',  # Descripción del banco o entidad de pago
        'fecha_factura': 'object',  # Fecha de emisión de la factura
        'estado_firma': 'object',  # Estado de la firma
        'serial_firma': 'object',  # Serial de la firma electrónica
        'fecha_inicio_tramite': 'object',  # Fecha de inicio del trámite
        'fecha_caducidad': 'object',  # Fecha de caducidad original
        'MEMBER_of_operador': 'object',  # Miembro del operador
        'valorfactura': 'float',  # Valor total de la factura
        'medio_contacto': 'category',  # Medio de contacto (vacío)
        'Ciudad': 'object',  # Cantón o ciudad
        'correo': 'object',  # Correo electrónico del usuario
        'telefono': 'object',  # Teléfono del usuario
        'Nombre': 'object',  # Nombre completo del usuario
        'flujo': 'category'  # Flujo del trámite (vacío)
    }
    float_columns = ['valorfactura']

    # Consulta SQL
    # Se consideran unicametne las firmas vigentes, las firmas suspendidas y revocadas no son consideradas para este
    # analisis, puesto que se requiere unicamente las firmas que estan caducandose en un periodo de 6 meses

    # Create a pipeline
    firmas_portal_pipeline = Pipeline([
        ('extract_data',
         dbm.DataExtractorPortal(database='Portal', query_path=query_path, dtypes_variables=column_types,
                                 float_columns=float_columns, save_request=True)),
        #('Cargar el CSV', fgp.CSVLoaderTransformer(r'C:\Users\cbenalcazar\PycharmProjects\BussinesIntelligenceFunctions\DatabasesConsultas\FirmasPortal.csv')),
        ('Converte Date DateTime', fgp.DTypeDateTime([
            'fecha_aprobacion', 'fecha_expedicion',
            'fecha_factura', 'fecha_inicio_tramite', 'fecha_caducidad',
        ])),
        ('Replace_values_Renovacion', fgp.ReplaceValues('producto', ['No', 'Si'], ['Emisión', 'Renovación'])),
        ('Replace_values_estado', fgp.ReplaceValues('estado_firma', [0, 7, 8],
                                                    ['Solicitud en Revision', 'Solicitud en Revision',
                                                     'Rechazo Documentos'])),
        ('Replace_values_formapago', fgp.ReplaceValues('forma_pago', ["20", "16", "19"],
                                                       ['Transferencia/Deposito', 'Pago en Linea', "Pago en Linea"])),
        ('Add Column ', fgp.DuplicarColumnaOtroNombre(column_name='estado_tramite', column_name_aux='estado_firma')),
        ('Change Variable Type to Categoritcal', dtt.DTypeCategorical(['producto', 'estado_firma', 'forma_pago'])),
        # Agregada puesto que todos los registros extraidos fueron finalizados
    ])
    ########CAMBIAR EN LOS CALCULOS DE DIA DE RENOVACION POR LA FECHA DE FACTURACION EN LUGAR INCIO TRMAITE
    # Ejecutar el pipeline de ETL
    extracted_data = firmas_portal_pipeline.fit_transform(None)
    return extracted_data


def database_concat_portal_camunda():
    folder_query = r"C:\Users\cbenalcazar\PycharmProjects\BussinesIntelligenceFunctions\Reporte_Renovacion_Firmas\Consultas SQL"
    query_camunda_archivo = os.path.join(folder_query, "Solicitudes_Proceso_Camunda.sql")
    query_portal_archivo = os.path.join(folder_query, "Solicitudes_Proceso_Portal.sql")
    df_portal = etl_portal_database(query_portal_archivo)
    df_camunda = etl_camunda_database(query_camunda_archivo)

    concatenated_pipelines = Pipeline([
        ('concatenate', sf.ConcatenatedDataFrames(df1=df_camunda, df2=df_portal, axis=0)),
        ('Actualizacion de Date FACT por fecha Init Tramite en campos vacios',
         fgp.FillNAValues('fecha_factura', 'fecha_inicio_tramite')),
        ('Replace_Guion_Nulls_RUC', fgp.ReplaceValues('ruc', ['-', ''], [np.nan, np.nan])),
        ('Create_Column_key', fgp.CrearKeyWithCedulaRucTP('cedula', 'ruc', 'tipofirma', 'key')),
        ('Define Origen de la Firma', sf.DefineRequestOrigen('MEMBER_of_operador', 'correo')),
        ('Count the amount of null Values', fgp.CountNullValuesRow('nulls_count')),
    ])

    df_portal_camunda = concatenated_pipelines.fit_transform(None)
    return df_portal_camunda


def main():
    print("main")
    df = database_concat_portal_camunda()
    # Definir el path del archivo
    directory = r'C:/Users/cbenalcazar/PycharmProjects/BussinesIntelligenceFunctions/DatabasesConsultas'
    filename = f'Procesos Pendientes.csv'
    fgp.save_dataframe_csv(df, directory, filename)



if __name__ == '__main__':
    main()
