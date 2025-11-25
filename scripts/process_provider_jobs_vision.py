import os
import requests
from google.cloud import vision
from supabase import create_client, Client


SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Google Vision client
client_vision = vision.ImageAnnotatorClient()


def ocr_google(url):
    resp = requests.get(url)
    image = vision.Image(content=resp.content)

    response = client_vision.text_detection(image=image)

    if response.error.message:
        raise Exception(response.error.message)

    return response.text_annotations[0].description if response.text_annotations else ""


def parse_items(text):
    import re

    results = []
    for line in text.split("\n"):
        clean = line.strip()
        if len(clean) < 3:
            continue

        # precio: 1.99  ó  1,99  ó  1.99€
        m = re.search(r"(\d+[.,]\d{1,2})\s*€?$", clean)
        if not m:
            continue

        precio = float(m.group(1).replace(",", "."))

        # nombre sin precio final
        nombre = clean.replace(m.group(1), "").replace("€", "").strip()

        if len(nombre) < 2:
            continue

        results.append({
            "nombre": nombre,
            "precio": precio,
            "unidad_base": "unidad",
            "cantidad_presentacion": 1,
            "formato_presentacion": "",
            "iva_porcentaje": 10,
            "merma": 0
        })

    return results


def process_job(job):
    print(f"🔎 Procesando página {job['numero_pagina']}…")

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
        supabase.table("proveedor_listas_jobs").update({"estado": "error", "error": str(e)}).eq("id", job["id"]).execute()


def main():
    jobs = supabase.table("proveedor_listas_jobs") \
        .select("*") \
        .eq("estado", "pendiente") \
        .order("numero_pagina", asc=True) \
        .execute().data

    if not jobs:
        print("No pending jobs.")
        return

    print(f"🟦 {len(jobs)} páginas pendientes detectadas")

    for job in jobs:
        process_job(job)

    print("✔ Finalizado")


if __name__ == "__main__":
    main()
