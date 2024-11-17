import pandas as pd
import io
import os
import sys

# Check if the user provided a filename as an argument
if len(sys.argv) < 2:
    print("Error: No file specified.")
    sys.exit(1)

# Get the filename from the command line argument
file_name = sys.argv[1]
directory = os.path.dirname(file_name)

# Словарь для хранения DataFrame из секций
dfs = {}

# Чтение и обработка файла
try:
    with open(file_name, 'r') as f:
        write_key = None
        buffer = io.StringIO()

        for line in f:
            # Если строка обозначает начало новой секции
            if line.startswith('['):
                # Сохранить предыдущую секцию, если она есть
                if write_key:
                    buffer.seek(0)
                    dfs[write_key] = pd.read_csv(buffer, sep='\t', header=0)
                # Обновить имя текущей секции
                write_key = line.strip('[]\n')
                buffer = io.StringIO()  # Новый буфер для следующей секции
            else:
                # Добавить строку в текущий буфер
                buffer.write(line)

        # Сохранить последнюю секцию
        if write_key:
            buffer.seek(0)
            dfs[write_key] = pd.read_csv(buffer, sep='\t', header=0)

except FileNotFoundError:
    print(f"Error: The file {file_name} does not exist.")
    sys.exit(1)

# Создать папку для сохранения файлов
output_folder = directory
os.makedirs(output_folder, exist_ok=True)
columns_to_remove = ['Definition', 'Ontology_Component', 'Ontology_Process', 'Ontology_Function', 'Synonyms', 'Obsolete_Probe_Id', 'Probe_Sequence']

# Сохранение каждой таблицы в отдельный файл
for section, df in dfs.items():
    if section == "Probes":
        output_file = os.path.join(output_folder, f"{section}.tsv")
        df.to_csv(output_file, sep='\t', index=False)  # Сохранить в TSV-формате
        df = df.drop(columns=columns_to_remove)
        print(f"Сохранено: {output_file}")
        output_file = os.path.join(output_folder, f"{section}_short.tsv")
        df.to_csv(output_file, sep='\t', index=False)  # Сохранить в TSV-формате
    else:
        output_file = os.path.join(output_folder, f"{section}.tsv")
        df.to_csv(output_file, sep='\t', index=False)  # Сохранить в TSV-формате
    print(f"Сохранено: {output_file}")
