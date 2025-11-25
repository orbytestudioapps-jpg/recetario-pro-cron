import os
import requests
from io import BytesIO
from paddleocr import PaddleOCR
from pdf2image import convert_from_bytes
from supabase import create_client
import re

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

ocr = PaddleOCR(
    use_angle_cls=True,
    lang="es",
    use_gpu=False
)


def extract_items_from_text(text):
    """
    Parser simple: extrae "nombre + precio"
    Ejemplo: "Ajos pelados bolsa 1kg 4.79"
    """
    items = []
    lines = text.split("\n")

    for l in lines:
        l = l.strip()
        if len(l) < 3:
            continue

        m = re.search(r"(\d+[.,]\d{2})$", l)
        if not m:
            continue

        price = float(m.group(1).replace(",", "."))
        name = l.replace(m.group(1), "").strip()

        items.append({
            "nombre": name,
            "precio": price,
            "unidad": "unidad",
            "cantidad": 1,
        })

    return items


def process_job(job):
    print(f"📄 Procesando job {job['id']} página {job['numero_pagina']}")

    # 1. Descargar la imagen desde Storage
    resp = requests.get(job["archivo_url"])
    if resp.status_code != 200:
        raise Exception("No se pudo descargar archivo_url")

    img_bytes = BytesIO(resp.content)

    # 2. OCR
    result = ocr.ocr(img_bytes, cls=True)

    # Convertir OCR a texto continuo
    raw_text = "\n".join([text for line in result for (_, (text, _)) in line])

    # 3. Parsear productos
    items = extract_items_from_text(raw_text)
    print(f"🟦 Items extraídos: {items}")

    # 4. Insertar items en proveedor_listas_items
    for it in items:
        supabase.table("proveedor_listas_items").insert({
            "proveedor_id": job["proveedor_id"],
            "organizacion_id": job["organizacion_id"],
            "nombre": it["nombre"],
            "precio": it["precio"],
            "unidad_base": "unidad",
            "cantidad_presentacion": 1,
            "formato_presentacion": "",
            "iva_porcentaje": 10,
            "merma": 0,
            "creado_desde_archivo": job["lista_id"]
        }).execute()

    # 5. Marcar job como procesado
    supabase.table("proveedor_listas_jobs").update({
        "estado": "procesado"
    }).eq("id", job["id"]).execute()

    print("✅ Job procesado OK")


def update_progress(lista_id):
    a = supabase.table("proveedor_listas_jobs").select("*", count="exact") \
        .eq("lista_id", lista_id).eq("estado", "procesado").execute()

    b = supabase.table("proveedor_listas_jobs").select("*", count="exact") \
        .eq("lista_id", lista_id).execute()

    procesados = a.count or 0
    total = b.count or 0

    estado = "procesado" if procesados == total else "procesando"

    supabase.table("proveedor_listas").update({
        "lotes_procesados": procesados,
        "total_lotes": total,
        "estado": estado
    }).eq("id", lista_id).execute()

    print(f"📦 Progreso lista {lista_id}: {procesados}/{total}")


if __name__ == "__main__":
    print("🚀 Iniciando procesamiento OCR...")

    # Obtener TODOS los jobs pendientes
    jobs = supabase.table("proveedor_listas_jobs") \
        .select("*") \
        .eq("estado", "pendiente") \
        .order("numero_pagina", desc=False) \
        .execute()

    if not jobs.data:
        print("No hay jobs pendientes.")
        exit(0)

    for job in jobs.data:
        try:
            process_job(job)
            update_progress(job["lista_id"])
        except Exception as e:
            print("❌ Error:", e)
            supabase.table("proveedor_listas_jobs").update({
                "estado": "error",
                "error": str(e)
            }).eq("id", job["id"]).execute()
