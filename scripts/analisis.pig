
datos = LOAD '/input/tarea3/llm.txt' AS (linea:chararray);
STOPWORDS = LOAD '/input/tarea3/stopwords.txt' AS (stopword:chararray);

-- 1. LIMPIEZA DE PUNTUACIÓN y TOKENIZACIÓN
lineas_limpias = FOREACH datos GENERATE REPLACE(linea, '[^a-zA-Z\\s]', ' ') AS linea_limpia; 

palabras_raw = FOREACH lineas_limpias GENERATE FLATTEN(TOKENIZE(linea_limpia)) AS palabra;

-- 2. LIMPIEZA: Pasar a minúsculas
palabras_lower = FOREACH palabras_raw GENERATE LOWER(palabra) AS palabra;


-- 3. FILTRADO (Basura y Stopwords)
palabras_limpias_basicas = FILTER palabras_lower BY 
    (palabra IS NOT NULL) AND
    (SIZE(palabra) > 1) AND                   
    (palabra MATCHES '[a-z]+') AND
    (palabra != 'contexto') AND 
    (palabra != 'respuesta');


JOINED_DATA = JOIN palabras_limpias_basicas BY palabra LEFT OUTER, STOPWORDS BY stopword;

--  Filtrar las stopwords: 
CLEAN_TOKENS = FILTER JOINED_DATA BY stopword IS NULL;

palabras_filtradas = FOREACH CLEAN_TOKENS GENERATE palabras_limpias_basicas::palabra AS palabra;


-- 4. CONTEO (MapReduce)
agrupadas = GROUP palabras_filtradas BY palabra;
conteo = FOREACH agrupadas GENERATE group AS palabra, COUNT(palabras_filtradas) AS cantidad;

-- 5. ORDENAR POR FRECUENCIA
ordenado = ORDER conteo BY cantidad DESC;

-- 6. GUARDAR RESULTADO

STORE ordenado INTO '/output/tarea3/resultado_llm';
