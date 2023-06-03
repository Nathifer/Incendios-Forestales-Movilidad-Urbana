# Incendios Forestales en Cataluña

## Obtención del conjunto de datos 
 
Los datos utilizados en el en la investigación provienen de las siguientes fuentes de datos abiertas y anonimizadas:

1.  [Servicio Meteorológico de Catalunya (Meteocat):](https://gitlab.com/Nathifer/incendios-forestales-en-cataluna/-/blob/main/EXTRACCION/METEOCAT/1.1_METEOCAT_carga.ipynb), que nos proporciona:
    + Datos meteorológicos: variables medidas con una frecuencia diaria, registrados en todas las estaciones de la red de estaciones meteorológicas automáticas de Catalunya.
    + Ocurrencia de incendios:  registros de los incendios forestales reportados de Cataluña, el municipio y comarca donde se produce, la fecha y las hectáreas afectadas. Se crea la variable Incendio que nos dirá si para un día determinado hubo incendio “1” o no “0”.

2.  [Agencia Estatal de meteorología (Aemet):](https://gitlab.com/Nathifer/incendios-forestales-en-cataluna/-/blob/main/EXTRACCION/AEMET/1.0_AEMET_carga.ipynb), nos proporciona valores meteorológicos diarios en distintas estaciones de toda España.
   
3.  [Gencat - Departamento de Agricultura:](https://gitlab.com/Nathifer/incendios-forestales-en-cataluna/-/blob/main/EXTRACCION/GENCAT/1.4_GENCAT_carga.ipynb) tabla de atributos del Mapa .SHP de Riesgo de Incendios Forestales en la comunidad de Cataluña.
   
4.  [Instituto de Estadística de Cataluña:](https://gitlab.com/Nathifer/incendios-forestales-en-cataluna/-/blob/main/EXTRACCION/IDESCAT/1.3_IDESCAT_carga.ipynb) contempla la tabla de Superficie y pendientes de las Comarcas de Cataluña.

5.  [Instituto Nacional de Estadística:](https://gitlab.com/Nathifer/incendios-forestales-en-cataluna/-/tree/main/EXTRACCION/INE)
    + Codigos de municipios Ine-Mitma 
    + Codigos de Comunidades autonomas y provincias

6.  [Ministerio de Transporte y Movilidad Urbana:](https://gitlab.com/Nathifer/incendios-forestales-en-cataluna/-/blob/main/EXTRACCION/MITMA/1.5_MITMA_Movilidad_carga.ipynb), que nos proporciona datos sobre la movilidad urbana durante el periodo de 02/2020 – 05/2021 y está constituida datos anonimizados asociados a los registros de conexión de los dispositivos móviles con la red de telefonía móvil.


# Transformacion de Datos
 1. Se realiza un pivot de la tabla de meteocat para unir las variables del clima con los valores medidos por las estaciones
 2. Se unen tablas de valores climatologicos de aemet y meteocat 
 3. Se filtra la comunidad autonoma de Cataluña
 4. Se imputan valores del codigo de municipio de la tabla del INE
 5. Se añade la variable de Altitud  
 6. Se añade la variable Pendiente 
 7. Se añade la variable Riesgo de incendio
 8. Se realizan transformaciones en los datos de Movilidad mitma como:
    + Incluimos el codigo_destino del municipio 
    + Incluimos el codigo_origen del municipio 


# Exploracion de Datos:

1. Valores faltantes /nulos
2. Tipos de datos en cada variable
3. Desbalanceo de Clases
4. Correlacion  
5. Estacionalidad (Autocorrelacion) https://www.datainsightonline.com/post/cross-correlation-with-two-time-series-in-python


## Red Neuronal Artificial  LSTM

La aplicación de Redes Neuronales Artificiales a la predicción de series temporales especificamente redes Long Short Term Memory (LSTM) se realiza en esta investigación de acuerdo a las siguientes etapas: 

1. Búsqueda de las variables de entrada

Esta etapa tiene como objetivo identificar las variables de entrada en la red neuronal.

2. Preparación del conjunto de datos

Esta etapa tiene como objetivo realizar la division y  normalización de datos en el intervalo [0, 1].

3. Creación de la red

Esta etapa tiene como objetivo determinar cada elemento que compone la arquitectura de la red. 

4. Entrenamiento

En esta etapa se define el algoritmo de entrenamiento y los parámetros de configuración propios de éste. 

5. Validación

Esta etapa tiene como objetivo realizar la validación del proceso de aprendizaje de la red. Se presenta a la red el conjunto de datos seleccionados para este fin y se obtienen los valores de la predicción del siguiente periodo para cada patrón de datos.

6. Cálculo de los factores de comparación

El objetivo de esta etapa consiste en calcular los factores que serán utilizados en el análisis de los resultados al comparar los distintos modelos  obtenidos a partir de la inclusion de variables predictoras y elegir la más efectiva en la predicción.

Para llevar a cabo esta tarea se obtienen los siguientes factores:

    + Exahustuvidad      
    + Presicion 
    + Exactitud
    + Representación gráfica de la matriz de confusion.
  

## Modelos Xgboost

Extreme Gradient Boosting (XGBoost) es un método basado en un clasificador de árboles de decisión los cuales se usan como un modelo débil que mejoran continuamente de los cuales a partir de estos se crean las predicciones. El XGBoost introduce el término de regularización en la función objetivo para evitar el sobreajuste.

Para este modelo hemos utilizado número de estimadores 500, lo que quiere decir la cantidad de árboles que utiliza el modelo,  una profundidad máxima que pueden alcanzar los árboles se ha fijado entre 5 y 20, la  tasa de aprendizaje la hemos fijado en 0.0001 para reducir el riesgo de overfitting y considerando que la cantidad de arboles fijados es suficiente. También, se especifica el objetivo de aprendizaje correspondiente a binary:logistic para clasificación binaria.
