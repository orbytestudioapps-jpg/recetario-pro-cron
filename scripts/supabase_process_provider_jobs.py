import easyocr
import requests
from supabase import create_client
import os

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

reader = easyocr.Reader(['es'], gpu=False)

def ocr_image(url):
    print("🧠 OCR leyendo:", url)
    resp = requests.get(url)
    text = reader.readtext(resp.content, detail=0)
    return "\n".join(text)

def parse_items(text):
    lines = text.split("\n")
    items = []
    for line in lines:
        parts = line.strip().split(" ")
        if len(parts) < 2:
            continue
        # Buscar precio al final
        for token in reversed(parts):
            token2 = token.replace(",", ".")
            try:
                price = float(token2)
                name = " ".join(parts[:-1]).strip()
                if len(name) > 1:
                    items.append({
                        "nombre": name,
                        "precio": price,
                        "cantidad": 1,
                        "unidad": "unidad"
                    })
                break
            except:
                continue
    return items

def run():
    print("🔍 Buscando jobs pendientes...")
    jobs = supabase.table("proveedor_listas_jobs") \
        .select("*") \
        .eq("estado", "pendiente") \
        .order("numero_pagina") \
        .execute()

    if len(jobs.data) == 0:
        print("No hay jobs pendientes")
        return

    print(f"📦 {len(jobs.data)} jobs encontrados")

    for job in jobs.data:
        print(f"➡ Procesando página {job['numero_pagina']}...")

        supabase.table("proveedor_listas_jobs").update({
            "estado": "procesando"
        }).eq("id", job["id"]).execute()

        try:
            text = ocr_image(job["archivo_url"])
            items = parse_items(text)

            for item in items:
                supabase.table("proveedor_listas_items").insert({
                    "proveedor_id": job["proveedor_id"],
                    "organizacion_id": job["organizacion_id"],
                    "nombre": item["nombre"],
                    "precio": item["precio"],
                    "unidad_base": item["unidad"],
                    "cantidad_presentacion": item["cantidad"],
                    "formato_presentacion": "",
                    "iva_porcentaje": 10,
                    "merma": 0,
                    "creado_desde_archivo": job["lista_id"]
                }).execute()

            supabase.table("proveedor_listas_jobs").update({
                "estado": "procesado"
            }).eq("id", job["id"]).execute()

        except Exception as e:
            supabase.table("proveedor_listas_jobs").update({
                "estado": "error",
                "error": str(e)
            }).eq("id", job["id"]).execute()


if __name__ == "__main__":
    run()
