# per testare seleziona tutto, tasto dx, e fai 'run in interactive window'
import json
import pandas as pd
import os

# Percorso del file JSON
json_path = os.path.join("bronze", "salesforce_response.json")

# Caricamento del JSON
with open(json_path, "r", encoding="utf-8") as f:
    data = json.load(f)

# Identifica info utili nel json per capire cosa estrarre dalle chiavi
# Estrae tutti i field da tutti gli oggetti contenutl nella chiave "dataStreams"

# Lista per contenere tutte le righe da scrivere
all_rows = []

# Estrai ogni data stream
## ogni ciclo raccoglie determinate info che stanno su quel livello di indentazione

for stream in data.get("dataStreams", []):
    data_stream_type = stream.get("dataStreamType", "") # 1
    data_access_mode = stream.get("dataAccessMode", "")
    status = stream.get("status", "")
    stream_name = stream.get("name", "")
    stream_label = stream.get("label", "")
    lastRunStatus = stream.get("lastRunStatus", "")
    lastRefreshDate = stream.get("lastRefreshDate", "")
    lastAddedRecords = stream.get("lastAddedRecords", "")
    lastProcessedRecords = stream.get("lastProcessedRecords", "")
    totalRecords = stream.get("totalRecords", "")
    category = stream.get("dataLakeObjectInfo", {}).get("category", "") # 2
    fields = stream.get("dataLakeObjectInfo", {}).get("dataSpaceInfo", []) # 3

    for field in fields:
        all_rows.append({
            # prendi quello che trovi sopra
            "Data Stream Type": data_stream_type,
            "Data Access Mode": data_access_mode,
            "Stream Name": stream_name,
            "Stream Label": stream_label,
            "Category": category,
            "Status": status,
            "Last Run Status": lastRunStatus,
            "Last Refresh Date": lastRefreshDate,
            "lastAddedRecords": lastAddedRecords,
            "lastProcessedRecords": lastProcessedRecords,
            "Total Records": totalRecords,
            # get di quello che trovi nel livello
            "Data Space Label": field.get("label", ""),
            "Data Space Name": field.get("name", "")
        })

# Crea il DataFrame finale
df = pd.DataFrame(all_rows)

# Mostra le prime righe
#print(df.head())

# Percorso in cui salvare il CSV
output_path = os.path.join("bronze", "data_streams_fields.csv")

# Salvataggio in CSV
df.to_csv(output_path, index=False, encoding="utf-8-sig")

print(f"✅ File CSV salvato in: {output_path}")