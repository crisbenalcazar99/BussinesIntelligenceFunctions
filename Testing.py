import sys
sys.path.append(r"C:\Users\rtoledo\PycharmProjects\ETL_BI")
from Reporte_Renovacion_Firmas.Pipelines import FirmasCamunda
cf.expiration_date_definition(dataset, "fecha_activacion", "FechaCaducidad", "fecha_aprob", "anos")

# 'dataset' contiene los datos de entrada para este script