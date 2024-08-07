# This is a sample Python script.
import pandas as pd


# Press Mayús+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


def print_hi(name):
    # Lista de los correos de los preferenciales
    df_correos_pref = pd.read_excel(
        r'E:\Inteligencia de Negocios\13. Renovaciones\INFO PREFERENCIALES.xlsx',
        sheet_name='Hoja1'
    )
    df_correos_pref['CORREOS CLIENTES '].str.strip()
    print(df_correos_pref.head(10))
    set_correos_pref = set(df_correos_pref['CORREOS CLIENTES '])


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
