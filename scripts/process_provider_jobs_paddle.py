import os
import json
import requests
from google.cloud import vision
from supabase import create_client, Client

# ================================
# 🔑 Autenticación Google Vision
# ================================
credentials_json = os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")

with open("google_credentials.json", "w") as f:
    f.write(credentials_json)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "google_credentials.json"

# ================================
# 🔧 Configuración Supabase
# ================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
client_vision = vision.ImageAnnotatorClient()


def ocr_google(url):
    """Descarga la imagen desde Storage y lee texto con Google Vision."""
    resp = requests.get(url)
    content = resp.content

    image = vision.Image(content=content)
    response = client_vision.text_detection(image=image)

    if response.error.message:
        raise Exception(response.error.message)

    return response.text_annotations[0].description if response.text_annotations else ""


def parse_items(text):
    """Detecta pares 'nombre + precio' usando heurística simple temporal."""
    lines = text.split("\n")
    results = []

    for line in lines:
        line = line.strip()
        if len(line) < 3:
            continue

        import re
        price = re.search(r"(\d+[.,]\d{1,2})$", line)
        if not price:
            continue

        p = float(price.group(1).replace(",", "."))
        name = line.replace(price.group(1), "").strip()

        if len(name) < 2:
            continue

        results.append({
            "nombre": name,
            "precio": p,
            "unidad_base": "unidad",
            "cantidad_presentacion": 1,
            "formato_presentacion": "",
            "iva_porcentaje": 10,
            "merma": 0,
        })

    return results


def process_job(job):
    print(f"🟦 Procesando página {job['numero_pagina']} — {job['archivo_url']}")

    # Marcar procesando
    supabase.table("proveedor_listas_jobs").update({"estado": "procesando"}).eq("id", job["id"]).execute()

    try:
        text = ocr_google(job["archivo_url"])
        items = parse_items(text)

        for item in items:
            item["proveedor_id"] = job["proveedor_id"]
            item["organizacion_id"] = job["organizacion_id"]
            item["creado_desde_archivo"] = job["lista_id"]

            supabase.table("proveedor_listas_items").insert(item).execute()

        supabase.table("proveedor_listas_jobs").update({"estado": "procesado"}).eq("id", job["id"]).execute()

    except Exception as e:
        print("❌ Error OCR:", e)
        supabase.table("proveedor_listas_jobs").update({
            "estado": "error",
            "error": str(e)
        }).eq("id", job["id"]).execute()


def main():
    jobs = (supabase.table("proveedor_listas_jobs")
            .select("*")
            .eq("estado", "pendiente")
            .order("numero_pagina", desc=False)
            .execute()
            .data)

    if not jobs:
        print("No pending jobs.")
        return

    print(f"🔍 {len(jobs)} jobs encontrados.")

    for job in jobs:
        process_job(job)

    print("✔ OCR finalizado.")


if __name__ == "__main__":
    main()
