import csv
import re

# Regex para manter apenas caracteres válidos UTF-8 "normais"
def remove_invalid_chars(text):
    if text is None:
        return text
    # remove caracteres não ASCII ou símbolos especiais (incluindo emojis)
    return re.sub(r'[^\x00-\x7F]+', '', text)

input_file = r"C:\Users\USER\Desktop\tables\olist_order_reviews_dataset.csv"
output_file = r"C:\Users\USER\Desktop\tables\olist_order_reviews_clean.csv"

with open(input_file, newline='', encoding='utf-8') as infile, \
     open(output_file, 'w', newline='', encoding='utf-8') as outfile:
    
    reader = csv.reader(infile)
    writer = csv.writer(outfile)
    
    # Remove caracteres inválidos linha por linha
    for row in reader:
        clean_row = [remove_invalid_chars(cell) for cell in row]
        writer.writerow(clean_row)

print("Arquivo limpo gerado com sucesso!")
