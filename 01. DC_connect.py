import requests
import json
import os
from consumer_details import CONSUMER_KEY, CONSUMER_SECRET, USERNAME, PASSWORD

# 1. Ottenere token
auth_url = "https://login.salesforce.com/services/oauth2/token"

payload = {
        "grant_type": "password",
        "client_id": CONSUMER_KEY,
        "client_secret": CONSUMER_SECRET,
        "username": USERNAME,
        "password": PASSWORD
    }

response = requests.post(auth_url, data=payload)

# check connessione
print(response)

# log
#print(response.text)

# estrazione valore token
response_dict = response.json()
access_token = response_dict.get("access_token")
#print(access_token)

# Endpoint dell'API che specifica richiesta
#url = "https://theinformationlabitaliasrl.my.salesforce.com/services/data/v63.0/ssot/data-spaces?limit=10" # elenco Data Space presenti
url = "https://theinformationlabitaliasrl.my.salesforce.com/services/data/v63.0/ssot/data-streams"

# Header con token di autenticazione
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

# Chiamata GET all'API
response = requests.get(url, headers=headers)

# Risultato
#print("Status:", response.status_code)
#print("Response JSON:", response.text)

# Percorso relativo della cartella di destinazione
output_dir = "bronze"
output_filename = "salesforce_response.json"
output_path = os.path.join(output_dir, output_filename)

# Crea la cartella 'bronze' se non esiste
os.makedirs(output_dir, exist_ok=True)


# Salva la risposta se è andata a buon fine
# per adesso tira fuori solo 10 data stream su 22, bisogna andare alla pagina successiva, da fare
if response.status_code == 200:
    data = response.json()
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    print(f"Risposta salvata in: {output_path}")
else:
    print("Errore nella richiesta:", response.text)
