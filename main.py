# import the modules
import AnonTechniques as ATechniques
import pandas as pd

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # Crea un dataframe a partir de un conjunto de datos separados por coma
    df = pd.read_csv('dataset.txt', sep=",")

    # Paso 0 -----------------------------------------------------------------------------------------------------------

    # Printea el inicio del programa
    print("[+] Procesando dataset...")
    # Printea el tipo de datos que almacena cada columna del conjunto de datos
    print(f"    [-] Columnas del conjunto de datos: {[entry for entry in df.dtypes.items()]}")

    # Paso 0 -----------------------------------------------------------------------------------------------------------
    # Paso 1 -----------------------------------------------------------------------------------------------------------

    # Pregunta al usuario que introduzca los identificadores del conjunto de datos, es decir, aquellos que pueden hacer
    # referencia a DNIs, matrículas, etc.
    identifiers = input("\n[+] Paso 1: Introduzca los identificadores (separados por \",\"): ")
    # Pregunta al usuario las opciones a realizar sobre los identificadores
    # La primera opción los pseudominiza, la segunda elimina la columna y la tercera no hace nada
    de_ident_flag = input("    [-] Paso 1 (opciones): De-identification (1) / Eliminar (2) /  No aplicar (3): ")
    identifiers = identifiers.split(",")

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
        df.drop(columns=identifiers)

        print("    [-] Paso 1: Se han eliminado todos los identificadores")
    elif de_ident_flag == "3":
        print("    [-] Paso 1: No se han aplicado acciones sobre los identificadores")

    # Paso 1 -----------------------------------------------------------------------------------------------------------
    # Paso 2 -----------------------------------------------------------------------------------------------------------

    # Pregunta al usuario si desea anonimizar el conjunto de datos entero o solo alguna columna
    # Se ha añadido esta posibilidad para añadir granularidad a la aplicación
    option_flag = input("\n[+] Paso 2 (opciones): Anonimizar conjunto entero (1) / Anonimizar columnas individuales (2) /  No aplicar (3): ")

    # Paso 2 -----------------------------------------------------------------------------------------------------------
    # Paso 3 -----------------------------------------------------------------------------------------------------------

    if option_flag == "1":
        # Pregunta al usuario por los QIs y el parámetro K
        # Realiza una K-Anonymization mediante el método "k_anonymization" del módulo "ATechniques"
        print("\n[+] Paso 3: Se anonimizará el conjunto de datos entero mediante K-Anonymization")
        QIs = input("    [-] Paso 3: Introduce los QIs (separados por \",\"): ")
        k = input("    [-] Paso 3: Introduce el parámetro K: ")
        df = ATechniques.k_anonymization(df, QIs, int(k))
    elif option_flag == "2":
        # Pregunta al usuario por el nombre de la columna a anonimizar
        print("\n[+] Paso 3: Se anonimizaran columnas individualmente")
        columns = input("    [-] Paso 3: Introduzca las columnas a anonimizar (separadas por \",\"): ")
        columns = columns.split(",")
        for col in columns:
            # Por cada columna pregunta al usuario la opción de anonimización
            option_flag = input(f"    [-] Paso 3 (columna {col}): Perturbation (1) / Generalization (2): ")

            if option_flag == "1":
                # Pregunta al usuario por el parámetro K
                k = input("    [-] Paso 3 (Perturbation): Introduce el parámetro K: ")
                df.sort_values(by=col, ascending=True)
                # Realiza una aggregation mediante el método "micro_aggregation" del módulo "ATechniques"
                df[col] = ATechniques.micro_aggregation(df[col], int(k))
                # Aleatoriza las columnas del dataset para incluir cierto "ruido"
                df.sample(frac=1)
            elif option_flag == "2":
                # Pregunta al usuario por el parámetro K
                k = input("    [-] Paso 3 (Perturbation): Introduce el parámetro K: ")
                # Realiza una generalization mediante el método "generalization" del módulo "ATechniques"
                df[col] = ATechniques.generalization(df, col, int(k))

    # Paso 4 -----------------------------------------------------------------------------------------------------------

    # Como resultado final, muestra el conjunto de datos una vez procesado
    print("\n[+] Paso 4: Mostrando conjunto de datos final anonimizado...")
    print(df)

    # Paso 4 -----------------------------------------------------------------------------------------------------------
