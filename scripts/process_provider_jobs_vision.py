import os
import requests
from google.cloud import vision
from supabase import create_client, Client
import re

# ================================
# 🔧 Configuración
# ================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Google Vision
client_vision = vision.ImageAnnotatorClient()


# ================================
# 🔍 OCR con Google Vision
# ================================
def ocr_google(url: str) -> str:
    resp = requests.get(url)
    content = resp.content

    image = vision.Image(content=content)
    response = client_vision.text_detection(image=image)

    if response.error.message:
        raise Exception(response.error.message)

    return response.text_annotations[0].description if response.text_annotations else ""


# ================================
# 📌 Parseo simple temporal
# ================================
def parse_items(text):
    lines = text.split("\n")
    results = []

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
            "merma": 0,
        })

    return results


# ================================
# 🔄 PROCESAR UN JOB
# ================================
def process_job(job):
    print(f"🟦 Procesando página {job['numero_pagina']} — {job['archivo_url']}")

    # Marcar como procesando
    supabase.table("proveedor_listas_jobs") \
        .update({"estado": "procesando"}) \
        .eq("id", job["id"]) \
        .execute()

    try:
        # OCR
        text = ocr_google(job["archivo_url"])

        # Parseo
        items = parse_items(text)

        # Insertar items
        for item in items:
            item["proveedor_id"] = job["proveedor_id"]
            item["organizacion_id"] = job["organizacion_id"]
            item["creado_desde_archivo"] = job["lista_id"]
            item["pagina"] = job["numero_pagina"]

            supabase.table("proveedor_listas_items").insert(item).execute()

        # Job procesado
        supabase.table("proveedor_listas_jobs") \
            .update({"estado": "procesado"}) \
            .eq("id", job["id"]) \
            .execute()

    except Exception as e:
        print("❌ Error OCR:", e)
        # Guardar error en columna existente
        supabase.table("proveedor_listas_jobs") \
            .update({"estado": "error"}) \
            .eq("id", job["id"]) \
            .execute()


# ================================
# 📊 ACTUALIZAR PROGRESO
# ================================
def actualizar_progreso(lista_id):
    procesados = supabase.table("proveedor_listas_jobs") \
        .select("*", count="exact") \
        .eq("lista_id", lista_id) \
        .eq("estado", "procesado") \
        .execute().count

    total = supabase.table("proveedor_listas_jobs") \
        .select("*", count="exact") \
        .eq("lista_id", lista_id) \
        .execute().count

    estado = "procesado" if procesados == total else "procesando"

    supabase.table("proveedor_listas") \
        .update({
            "lotes_procesados": procesados,
            "total_lotes": total,
            "estado": estado
        }) \
        .eq("id", lista_id) \
        .execute()

    print(f"📦 Progreso {procesados}/{total} — Estado: {estado}")


# ================================
# ▶ MAIN
# ================================
def main():
    jobs = supabase.table("proveedor_listas_jobs") \
        .select("*") \
        .eq("estado", "pendiente") \
        .order("numero_pagina", desc=False) \
        .execute().data

    if not jobs:
        print("No pending jobs.")
        return

    print(f"🔍 {len(jobs)} jobs encontrados.")

    # Procesar TODOS
    for job in jobs:
        process_job(job)
        actualizar_progreso(job["lista_id"])

    print("✔ OCR finalizado.")


if __name__ == "__main__":
    main()
