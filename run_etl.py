import subprocess
import sys

print("=== Démarrage de l'ETL complet ===")

# 1️⃣ Exécution des stagings (hash + UPSERT déjà gérés dans chaque staging)
staging_scripts = ["staging_d1.py", "staging_d2.py", "staging_d3.py"]

for script in staging_scripts:
    print(f"🔹 Exécution de {script} ...")
    subprocess.run([sys.executable, script], check=True)  # Utilise la même version de python

print(" Tous les stagings terminés")

# 2️⃣ Chargement dans le Data Warehouse
print(" Exécution du load DW ...")
subprocess.run([sys.executable, "load_dw.py"], check=True)

print("=== ETL complet terminé avec succès ===")
