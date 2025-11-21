/* scripts/analisis.pig */

-- ==========================================
-- CONFIGURACIÓN DE ENTRADA
-- Cambia 'humanos.txt' por 'llm.txt' según corresponda ejecutar
-- ==========================================
datos = LOAD '/input/tarea3/llm.txt' AS (linea:chararray);

-- 1. TOKENIZACIÓN: Separar frases en palabras
palabras_raw = FOREACH datos GENERATE FLATTEN(TOKENIZE(linea)) AS palabra;

-- 2. LIMPIEZA: Pasar a minúsculas
palabras_lower = FOREACH palabras_raw GENERATE LOWER(palabra) AS palabra;

-- 3. FILTRADO (Stopwords y basura)
-- Filtramos palabras cortas, signos y palabras comunes (stopwords)
palabras_filtradas = FILTER palabras_lower BY 
    (SIZE(palabra) > 3) AND 
    (palabra matches '[a-z]+') AND -- Solo letras, sin numeros
    (palabra != 'this') AND 
    (palabra != 'that') AND
    (palabra != 'have') AND
    (palabra != 'with') AND
    (palabra != 'what') AND
    (palabra != 'the') AND
    (palabra != 'your') AND
    (palabra != 'of') AND
    (palabra != 'contexto') AND -- Palabra que suele aparecer en los dumps
    (palabra != 'respuesta');

-- 4. CONTEO (MapReduce)
agrupadas = GROUP palabras_filtradas BY palabra;
conteo = FOREACH agrupadas GENERATE group AS palabra, COUNT(palabras_filtradas) AS cantidad;

-- 5. ORDENAR POR FRECUENCIA
ordenado = ORDER conteo BY cantidad DESC;

-- 6. GUARDAR RESULTADO
-- Cambia el nombre de la carpeta de salida para no sobrescribir
-- Ejemplo: 'output_humanos' o 'output_llm'
STORE ordenado INTO '/output/tarea3/resultado_llm';