import json

import pandas as pd
import requests


def AccessDashboards():
    #url = 'http://127.0.0.1:8000/api/v1/admin/reports/access'
    #response = requests.get(url)

    path_file = r"C:\Users\cbenalcazar\PycharmProjects\Intelligences_Microservices\data.json"
    with open(path_file, 'r') as file_json:
        data = json.load(file_json)
    # Verificar si la solicitud fue exitosa
    if True:
    #if response.status_code == 200:
        # Cargar el contenido de la respuesta JSON
        #data = response.json()

        # Crear una lista para almacenar las filas transformadas
        rows = []

        # Iterar sobre cada reporte
        for report in data['report']:
            report_id = report['id']
            report_name = report['name']
            report_type = report['type']

            # Iterar sobre cada subreporte
            for subreport in report['reports']:
                subreport_id = subreport['id']
                subreport_name = subreport['name']
                subreport_type = subreport['reportType']

                # Iterar sobre cada acceso de usuario
                for user_access in subreport['user_access']:
                    # Crear una fila para cada combinación de reporte, subreporte y acceso de usuario
                    rows.append({
                        'Workspace ID': report_id,
                        'Workspace Name': report_name,
                        'Workspace Type': report_type,
                        'Report ID': subreport_id,
                        'Report Name': subreport_name,
                        'Report Type': subreport_type,
                        'User Access Permission': user_access.get('reportUserAccessRight', ''),
                        'Email Address': user_access.get('emailAddress', 'emailWrong'),
                        'Username': user_access.get('displayName', ''),
                        'PrincipalType': user_access.get('principalType', '')
                    })

        # Convertir la lista de filas en un DataFrame de pandas
        df = pd.DataFrame(rows)
        df.to_excel(r'E:\Inteligencia de Negocios\37. Reporte Accesos\Report_Acceso_Dashboards.xlsx', index=False)
    else:
        print(f"Error en la solicitud: {response.status_code}")


if __name__ == "__main__":
    print("main")
    AccessDashboards()