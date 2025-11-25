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

    lines = [l.strip() for l in text.split("\n") if l.strip()]
    items = []

    # ==========================================================
    # 🔍 1) DETECCIÓN DE TABLA HORIZONTAL
    # ==========================================================
    header_keywords = ["CÓDIGO", "CODIGO", "FORMATO", "PVP", "€/KG", "€/kg"]

    if any(h.lower() in text.lower() for h in header_keywords):
        print("🔎 Página detectada como TABLA → usando parser horizontal")

        fila_re = re.compile(
            r"^(?P<codigo>[A-Za-z0-9\-]+)\s+"
            r"(?P<nombre>.+?)\s+"
            r"(?P<formato>\d.*?(Kg|kg|g|G|Caja|caja|x\s*\d).*?)\s+"
            r"(?P<pvp_unidad>\d+[.,]\d{1,2})\s*€?/Kg?"
        )

        for line in lines:
            m = fila_re.search(line)
            if m:
                nombre = m.group("nombre").strip()
                precio = float(m.group("pvp_unidad").replace(",", "."))
                formato = m.group("formato")

                items.append({
                    "nombre": nombre,
                    "precio": precio,
                    "unidad_base": "kg",
                    "cantidad_presentacion": 1,
                    "formato_presentacion": formato,
                    "iva_porcentaje": 10,
                    "merma": 0,
                })

        print(f"🟩 ITEMS extraídos en modo horizontal: {len(items)}")
        return items

    # ==========================================================
    # 🔍 2) PARSER VERTICAL CLÁSICO
    # ==========================================================
    print("🔎 Página detectada como LISTA VERTICAL → usando parser vertical")

    precio_re = re.compile(r"(\d+[.,]\d{1,2})\s*€?")

    i = 0
    while i < len(lines) - 2:
        nombre = lines[i]
        formato = lines[i + 1]
        linea_precio = lines[i + 2]

        precio_match = precio_re.search(linea_precio)

        if precio_match:
            precio = float(precio_match.group(1).replace(",", "."))

            items.append({
                "nombre": nombre,
                "precio": precio,
                "unidad_base": "unidad",
                "cantidad_presentacion": 1,
                "formato_presentacion": formato,
                "iva_porcentaje": 10,
                "merma": 0,
            })

            i += 3
        else:
            i += 1

    print(f"🟩 ITEMS extraídos en modo vertical: {len(items)}")
    return items


# ================================
# 🔄 PROCESAR UN JOB
# ================================
def process_job(job):
    print(f"\n\n==============================")
    print(f"🟦 Procesando página {job['numero_pagina']}")
    print(f"URL: {job['archivo_url']}")
    print("==============================\n")

    supabase.table("proveedor_listas_jobs").update(
        {"estado": "procesando"}
    ).eq("id", job["id"]).execute()

    try:
        # Leer OCR
        text = ocr_google(job["archivo_url"])

        # Parsear items
        items = parse_items(text)

        if not items:
            print("⚠️ NO SE DETECTARON ITEMS EN ESTA PÁGINA")
        else:
            print(f"✔ Se detectaron {len(items)} items, INSERTANDO...")

        # Insertar cada item con logs detallados
        for idx, item in enumerate(items):
            item["proveedor_id"] = job["proveedor_id"]
            item["organizacion_id"] = job["organizacion_id"]
            item["creado_desde_archivo"] = job["lista_id"]
            item["pagina"] = job["numero_pagina"]

            print(f"\n➡️ INSERT {idx+1}/{len(items)} → {item['nombre']}")

            resp = supabase.table("proveedor_listas_items").insert(item).execute()

            print(f"   🟩 INSERT OK: {resp.data}")

        # Marcar job procesado
        supabase.table("proveedor_listas_jobs").update(
            {"estado": "procesado"}
        ).eq("id", job["id"]).execute()

        print("\n✅ Página procesada con éxito")

    except Exception as e:
        print("❌ ERROR EN JOB:", e)

        supabase.table("proveedor_listas_jobs").update(
            {
                "estado": "error",
                "error": str(e)
            }
        ).eq("id", job["id"]).execute()

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
