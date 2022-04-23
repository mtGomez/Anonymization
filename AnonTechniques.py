# import the modules
import time

import pandas as pd
import statistics
import random
import string
import re
import numpy as np

# Este método realiza un hash reversible dada una cadena de texto como entrada
# Básicamente computa la representación ASCII de sus carácteres y añade letras aleatorias para introducir cierto ruido
# El objetivo es que dado el mismo ID el hash compute una cadena algo diferente cada vez
def h1(text):
    result = ""
    [result := result+str(ord(c))+random.choice(string.ascii_letters) for c in text]
    return result

# Este método realiza un hash reversible dada una cadena de texto como entrada
# Básicamente da la vuelta a la cadena de texto --> "mirroring"
def h2(text):
    result = text[::-1]
    return result

# Revierte el hash computado con el método "h1"
def d_h1(text):
    text = re.split(r"[A-z]", text)[0:-1]
    result = ""
    [result := result+chr(int(c)) for c in text]
    return result

# Revierte el hash computado con el método "h2"
def d_h2(text):
    result = text[::-1]
    return result

# Hace el hash de un valor con la función "h1" y al resultado vuelve a aplicarle una función hash con el método "h2"
# Esta técnica es una forma avanzada conocida como "set of hashes"
# En este caso los hashes "h1" y "h2" son muy sencillos pero pueden complicarse sustancialmente
def de_identification(df_column):
    df_column_list = df_column.tolist()
    result = list()
    [result.append(h2(h1(str(value)))) for value in df_column_list]

    return result

# Realiza la función inversa del método anterior, es decir, dada una columna, convierte todas sus cadenas de texto
# Pseudonimizadas al identificador original
def identification(df_column):
    df_column_list = df_column.tolist()
    result = list()
    [result.append(d_h1(d_h2(value))) for value in df_column_list]

    return result

# Realiza la perturbación de valores numericos o la "agrupación" si se tratan de valores categoricos
def generalization(df, columnName, k):
    dataType = str(df[columnName].dtype)

    if dataType == "object":
        categoricalGeneralization = list()
        columnValues = list(set(df[columnName].tolist()))

        if len(columnValues) != 1:
            for (index, value) in enumerate(columnValues):
                if index+1 < len(columnValues):
                    categoricalGeneralization.append(value + "/" + columnValues[index + 1])

        df_column_copy = df[columnName].copy(deep=True)
        for index in range(0, len(df_column_copy)):
            row_value = df_column_copy[index]
            for category in categoricalGeneralization:
                if row_value in category:
                    df_column_copy[index] = category
                    break

        return df_column_copy
    else:
        minValue = df[columnName].min()
        maxValue = df[columnName].max()

        statusValue = minValue-k
        bindValues = list()

        while statusValue < (maxValue + k):
            bindValues.append(statusValue)
            statusValue = statusValue + k

        return pd.cut(x=df[columnName], bins=bindValues)

# Realiza la micro agregación de valores numericos
def micro_aggregation(df_column, k):
    df_column_list = df_column.tolist()

    for i in range(0, len(df_column_list), k):
        mean = statistics.mean(df_column_list[i:i + k])
        size = len(df_column_list[i:i + k])
        df_column_list[i:i + k] = [mean] * size

    return df_column_list

# Realiza una partición del conjunto de datos en "k" partes diferentes y realiza una anonimización de cada una de dichas
# partes por separado, de tal forma que siempre hay al menos k-1 valores anonimizados de igual forma
def k_anonymization(df, QIs, k):
    QIs = QIs.split(",")
    df = df.sort_values(QIs)

    indices = df.index
    df_partitions = []
    for i in range(k):
        partition = df.iloc[indices[i::k]].copy(deep=True)
        df_partitions.append(partition)

    for QI in QIs:
        option = input(f"    [-] Paso 3: Escoga método para la columna {QI}: Perturbation (1) / Generalization (2): ")

        if option == "1":
            for frame in df_partitions:
                frame[QI] = micro_aggregation(QI, k)
        else:
            for frame in df_partitions:
                frame[QI] = generalization(frame, QI, k)

    return pd.concat(df_partitions)
