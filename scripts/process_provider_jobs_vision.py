import os
import requests
from google.cloud import vision
from supabase import create_client


# ================================
# 🔧 Configuración Supabase
# ================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ================================
# 🔧 Cliente Google Vision OCR
# ================================
client_vision = vision.ImageAnnotatorClient()


# ================================
# 🔍 OCR con Vision API
# ================================
def ocr_google(url):
    resp = requests.get(url)
    image = vision.Image(content=resp.content)

    response = client_vision.text_detection(image=image)

    if response.error.message:
        raise Exception(response.error.message)

    return response.text_annotations[0].description if response.text_annotations else ""


# ================================
# 🧠 Parser simple temporal
# ================================
def parse_items(text):
    import re

    results = []
    lines = text.split("\n")

    for line in lines:
        line = line.strip()
        if len(line) < 3:
            continue

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
            "merma": 0
        })

    return results


# ================================
# 🏗 Procesar un Job
# ================================
def process_job(job):
    print(f"🟦 Procesando página {job['numero_pagina']} — {job['archivo_url']}")

    # Marcar como procesando
    supabase.table("proveedor_listas_jobs").update({"estado": "procesando"}).eq("id", job["id"]).execute()

    try:
        # OCR
        text = ocr_google(job["archivo_url"])

        # Parser
        items = parse_items(text)

        # Insertar productos
        for item in items:
            item["proveedor_id"] = job["proveedor_id"]
            item["organizacion_id"] = job["organizacion_id"]
            item["creado_desde_archivo"] = job["lista_id"]

            res = supabase.table("proveedor_listas_items").insert(item).execute()
            if res.error:
                print("❌ ERROR insert:", res.error)

        # Marcar procesado
        supabase.table("proveedor_listas_jobs").update({"estado": "procesado"}).eq("id", job["id"]).execute()

    except Exception as e:
        print("❌ Error OCR:", e)
        supabase.table("proveedor_listas_jobs").update({
            "estado": "error",
            "error": str(e)
        }).eq("id", job["id"]).execute()


# ================================
# 🚀 Main
# ================================
def main():
    # Buscar jobs pendientes
    res = supabase.table("proveedor_listas_jobs") \
        .select("*") \
        .eq("estado", "pendiente") \
        .order("numero_pagina", desc=False) \
        .execute()

    jobs = res.data

    if not jobs:
        print("No pending jobs.")
        return

    print(f"🔍 {len(jobs)} jobs encontrados.")

    for job in jobs:
        process_job(job)

    print("✔ OCR finalizado.")


if __name__ == "__main__":
    main()
