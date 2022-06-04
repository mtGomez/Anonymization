# import the modules
from __future__ import print_function
import AnonTechniques as ATechniques
import pandas as pd

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # Crea un dataframe a partir de un conjunto de datos separados por coma
    df = pd.read_csv('dataset.csv', sep=",")

    # Paso 0 -----------------------------------------------------------------------------------------------------------

    # Printea el inicio del programa
    print("                                                        _                 ")
    print("     /\                                     (_)        | | (_)            ")
    print("    /  \   _ __   ___  _ __  _   _ _ __ ___  _ ______ _| |_ _  ___  _ __  ")
    print("   / /\ \ | '_ \ / _ \| '_ \| | | | '_ ` _ \| |_  / _` | __| |/ _ \| '_ \ ")
    print("  / ____ \| | | | (_) | | | | |_| | | | | | | |/ / (_| | |_| | (_) | | | |")
    print(" /_/    \_\_| |_|\___/|_| |_|\__, |_| |_| |_|_/___\__,_|\__|_|\___/|_| |_|")
    print("                              __/ |                                       ")
    print("                             |___/                                        ")
    print("\n[+] Procesando dataset...")
    # Printea el tipo de datos que almacena cada columna del conjunto de datos
    
    print("    [-] Columnas del conjunto de datos")
    for i in range(df.columns.size):
        if df.dtypes[i] == "object":
            print(f"        {df.columns[i]}: (string)")
        else:
            print(f"        {df.columns[i]}: ({df.dtypes[i]})")

    # Paso 0 -----------------------------------------------------------------------------------------------------------
    # Paso 1 -----------------------------------------------------------------------------------------------------------

    # Pregunta al usuario que introduzca los identificadores del conjunto de datos, es decir, aquellos que pueden hacer
    # referencia a DNIs, matrículas, etc.
    id_correct = False
    print("\n[+] Paso 1:")
    while id_correct == False:
        id_correct = True
        identifiers = input("    [-] Introduzca los identificadores (separados por \",\"): ")
        identifiers = identifiers.replace(" ", "")
        identifiers = identifiers.split(",")
        
        for identifier in identifiers:
            if identifier not in df:
                id_correct = False
        
        if id_correct == False:
                print("        Ha introducido valores incorrectos, por favor introduzca valores válidos")

    # Pregunta al usuario las opciones a realizar sobre los identificadores
    # La primera opción los pseudominiza, la segunda elimina la columna y la tercera no hace nada
    print("    [-] Opciones: \n        1. De-identification \n        2. Eliminar \n        3. No aplicar")
    de_ident_flag = input("        Selección: ")

    if de_ident_flag == "1":
        for identifier in identifiers:
            # Llama al método "de_identification" del módulo "ATechniques" creado por nosotros
            # Este método se encarga de realizar el pseudónimo dado una cadena de texto, lo hace de forma que se pueda
            # revertir, mediante el método de "set of hashes" en el siguiente apartado: 5.6 ADVANCED PSEUDONYMISATION TECHNIQUES
            # del siguiente enlace: https://www.enisa.europa.eu/publications/pseudonymisation-techniques-and-best-practices
            df[identifier] = ATechniques.de_identification(df[identifier])

        print("    [-] Paso 1: Se han pseudonomizado todos los identificadores")
    elif de_ident_flag == "2":
        # Elimina la columna de los identificadores directamente
        df = df.drop(columns=identifiers)

        print("    [-] Paso 1: Se han eliminado todos los identificadores")
    elif de_ident_flag == "3":
        print("    [-] Paso 1: No se han aplicado acciones sobre los identificadores")

    # Paso 1 -----------------------------------------------------------------------------------------------------------
    # Paso 2 -----------------------------------------------------------------------------------------------------------

    # Pregunta al usuario si desea anonimizar el conjunto de datos entero o solo alguna columna
    # Se ha añadido esta posibilidad para añadir granularidad a la aplicación
    print("\n[+] Paso 2:")
    print("    [-] Opciones: \n        1. Anonimizar conjunto entero \n        2. Anonimizar columnas individuales \n        3. No aplicar")
    option_flag = input("        Selección: ")

    # Paso 2 -----------------------------------------------------------------------------------------------------------
    # Paso 3 -----------------------------------------------------------------------------------------------------------
    
    if option_flag == "1":
        # Pregunta al usuario por los QIs y el parámetro K
        # Realiza una K-Anonymization mediante el método "k_anonymization" del módulo "ATechniques"
        qi_correct = False
        print("\n[+] Paso 3:")
        print("    Se anonimizará el conjunto de datos entero mediante K-Anonymization")
        while qi_correct == False:
            qi_correct = True
            QIs = input("    [-] Introduce los QIs (separados por \",\"): ")
            QIs = QIs.replace(" ", "")
            QIs = QIs.split(",")
            for QI in QIs:
                if QI not in df:
                    qi_correct = False
            
            if qi_correct == False:
                    print("        Ha introducido valores incorrectos, por favor introduzca valores válidos")

        k = input("    [-] Introduce el parámetro K: ")
        df = ATechniques.k_anonymization(df, QIs, int(k))
    elif option_flag == "2":
        col_correct = False
        # Pregunta al usuario por el nombre de la columna a anonimizar
        print("    Se anonimizaran columnas individualmente")
        while col_correct == False:
            col_correct = True
            columns = input("    [-] Introduzca las columnas a anonimizar (separadas por \",\"): ")
            columns = columns.replace(" ", "")
            columns = columns.split(",")
            for column in columns:
                if column not in df:
                    col_correct = False
            
            if col_correct == False:
                    print("        Ha introducido valores incorrectos, por favor introduzca valores válidos")

        for col in columns:
            op_correct = False
            # Por cada columna pregunta al usuario la opción de anonimización
            print(f"    [-] Columna {col}:")
            while op_correct == False:
                print("        1. Perturbation \n        2. Generalization")
                option_flag = input(f"        Selección: ")
                data = str(df[col].dtype)
                if data == "object" and option_flag == "1":
                    print("        No es posible aplicar la perturbación con atributos categóricos")
                    print("    [-] Por favor seleccione un valor válido")
                    op_correct = False
                else:
                    op_correct = True

            if option_flag == "1":
                # Pregunta al usuario por el parámetro K
                print("    [-] Perturbation:")
                k = input("        Introduce el parámetro K: ")
                df.sort_values(by=col, ascending=True)
                # Realiza una aggregation mediante el método "micro_aggregation" del módulo "ATechniques"
                df[col] = ATechniques.micro_aggregation(df[col], int(k))
                # Aleatoriza las columnas del dataset para incluir cierto "ruido"
                df.sample(frac=1)
            elif option_flag == "2":
                # Pregunta al usuario por el parámetro K
                print("    [-] Generalization:")
                k = input("        Introduce el parámetro K: ")
                # Realiza una generalization mediante el método "generalization" del módulo "ATechniques"
                df[col] = ATechniques.generalization(df, col, int(k))

    # Paso 4 -----------------------------------------------------------------------------------------------------------

    # Como resultado final, muestra el conjunto de datos una vez procesado
    print("\n[+] Paso 4: Mostrando conjunto de datos final anonimizado...")
    print(df.columns)

    # Paso 4 -----------------------------------------------------------------------------------------------------------
