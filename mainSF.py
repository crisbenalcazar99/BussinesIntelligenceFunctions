# This is a sample Python script.
import pandas as pd
import Reporte_Renovacion_Firmas.pipelines_procesos_pendientes as pipeline_pendientes
import Reporte_Renovacion_Firmas.Pipelines as pipeline_renovaciones


def main():
    pipeline_renovaciones.main()
    pipeline_pendientes.main()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
