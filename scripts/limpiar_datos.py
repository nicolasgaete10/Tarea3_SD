import csv
import re

def clean_line(line):
    line = line.rstrip('\n').rstrip(';')
    
    if len(line) > 1 and line.startswith('"') and line.endswith('"'):
        line = line[1:-1] 
        line = line.replace('""', '"') 
    return line

f_humanos = open('humanos.txt', 'w', encoding='utf-8')
f_llm = open('llm.txt', 'w', encoding='utf-8')

exito = 0
errores = 0

with open('yahoo_respuestas_gemini.csv', 'r', encoding='latin-1') as f: 
    lines = f.readlines()
    
    for line in lines[1:]: 
        cleaned = clean_line(line)
        if not cleaned.strip(): continue
            
        try:
            reader = csv.reader([cleaned], delimiter=',')
            row = next(reader)
            
            if len(row) >= 5:
                resp_yahoo = row[3].replace('\n', ' ')
                resp_gemini = row[4].replace('\n', ' ')
                
                if resp_yahoo: f_humanos.write(resp_yahoo + '\n')
                if resp_gemini: f_llm.write(resp_gemini + '\n')
                exito += 1
            else:
                errores += 1
        except Exception as e:
            errores += 1

f_humanos.close()
f_llm.close()

print(f"Proceso terminado. Filas procesadas: {exito}. Errores/Saltados: {errores}")
print("Archivos generados: humanos.txt y llm.txt")