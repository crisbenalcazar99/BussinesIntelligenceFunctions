import json

import requests
import pandas as pd


def script_consulta_ruc():
    licencia_api = 'nvlkajsfdkajalasdkljaskd'
    archivo_rucs = pd.read_excel(r'C:\Users\cbenalcazar\PycharmProjects\BussinesIntelligenceFunctions\Consulta RUC\FIles\RUCS_EntidadesPublicas.xlsx')
    list_rucs = archivo_rucs['RUC']
    url = 'http://172.16.60.12:5022/api/consultar_ruc'
    list_consultas = []

    for index, ruc in enumerate(list_rucs):
        # Parámetros opcionales para la solicitud (si son necesarios)
        params = {
            'numero_ruc': ruc,
            'licencia': licencia_api
        }

        # Realiza la solicitud GET a la API
        response = requests.post(url, data=json.dumps(params))

        # Verifica si la solicitud fue exitosa
        if response.status_code == 200:
            # Convierte la respuesta JSON a un diccionario
            resultado_json = response.json()
            list_consultas.append(resultado_json)

            # Imprime el resultado del diccionario
            print(list_consultas)
        else:
            print(f"Error en la solicitud: {response.status_code}")

    df_consultas = pd.DataFrame(list_consultas)
    df_consultas.to_excel('C:\Users\cbenalcazar\PycharmProjects\BussinesIntelligenceFunctions\Consulta RUC\FIles\RUC_CONSULTAS.xlsx')


if __name__ == '__main__':
    script_consulta_ruc()
