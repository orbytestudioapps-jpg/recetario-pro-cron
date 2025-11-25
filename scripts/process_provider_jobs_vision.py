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
def parse_items_inteligente(text):

    # Normalizamos
    lines = [l.strip() for l in text.split("\n") if l.strip()]
    items = []

    precio_re = re.compile(r"(\d+[.,]\d{1,2})\s*€?")
    formato_re = re.compile(r"\b(\d+\s?(gr|kg|KG|UND|UND\.|Uds|bandeja|manojo))\b", re.IGNORECASE)
    lambweston_re = re.compile(r"^[A-Z]{2}\d{3,}")

    # ----------------------------------------------------------
    # 💡 DETECCIÓN AUTOMÁTICA DEL TIPO DE PÁGINA
    # ----------------------------------------------------------

    def es_tabla_horizontal(lines):
        columnas = 0
        for l in lines:
            partes = l.split()
            if len(partes) >= 3:
                columnas += 1
        return columnas > 6

    def es_lambweston(lines):
        return any(lambweston_re.match(l) for l in lines)

    def es_vertical_flexible(lines):
        precios = sum(1 for l in lines if precio_re.search(l))
        nombres = len(lines)
        return precios >= 1 and precios < nombres

    # ----------------------------------------------------------
    # 🔷 PARSER TIPO LAMBWESTON
    # ----------------------------------------------------------
    def parse_lambweston(lines):
        productos = []
        for i in range(len(lines)):
            l = lines[i]
            if lambweston_re.match(l):
                codigo = l
                nombre = lines[i + 1] if i + 1 < len(lines) else ""
                formato = lines[i + 2] if i + 2 < len(lines) else ""

                # Buscar precio kg y precio caja
                precio_lineas = lines[i + 3:i + 6]
                precio = None
                for pl in precio_lineas:
                    pm = precio_re.search(pl)
                    if pm:
                        precio = float(pm.group(1).replace(",", "."))
                        break

                if precio is None:
                    continue

                productos.append({
                    "nombre": nombre.replace('"', "").strip(),
                    "precio": precio,
                    "unidad_base": "unidad",
                    "cantidad_presentacion": 1,
                    "formato_presentacion": formato,
                    "iva_porcentaje": 10,
                    "merma": 0,
                })

        return productos

    # ----------------------------------------------------------
    # 🔶 PARSER TABLA HORIZONTAL GENÉRICO
    # ----------------------------------------------------------
    def parse_tabla_horizontal(lines):
        productos = []
        for l in lines:
            partes = [p.strip() for p in re.split(r"\s{2,}", l)]
            if len(partes) < 2:
                continue

            nombre = partes[0]
            precio = None
            formato = ""

            # Buscar precio en columnas
            for p in partes:
                pm = precio_re.search(p)
                if pm:
                    precio = float(pm.group(1).replace(",", "."))
                if formato_re.search(p):
                    formato = p

            if precio:
                productos.append({
                    "nombre": nombre,
                    "precio": precio,
                    "unidad_base": "unidad",
                    "cantidad_presentacion": 1,
                    "formato_presentacion": formato,
                    "iva_porcentaje": 10,
                    "merma": 0,
                })

        return productos

    # ----------------------------------------------------------
    # 🔸 PARSER VERTICAL FLEXIBLE (3 líneas O disperso)
    # ----------------------------------------------------------
    def parse_vertical(lines):
       productos = []
    i = 0

    precio_re = re.compile(r"(\d+[.,]\d{1,2})\s*€?")
    formato_re = re.compile(
        r"\b((\d+\s?)?(gr|kg|KG|und|UND|uds|Uds|bandeja|manojo))\b",
        re.IGNORECASE
    )

    while i < len(lines):

        linea = lines[i]

        # ============================================
        # 1) Buscar nombre + formato en la MISMA línea
        # Ej: "Granadas Kg"
        # ============================================
        partes = linea.split()

        nombre = None
        formato = None
        precio = None

        # Detectar formato dentro de la línea
        for p in partes:
            if formato_re.match(p):
                formato = p
                break

        # Si encontramos formato, el nombre es el resto
        if formato:
            nombre = linea.replace(formato, "").strip()

        # ============================================
        # 2) Buscar precio en la misma línea
        # ============================================
        pm = precio_re.search(linea)
        if pm:
            precio = float(pm.group(1).replace(",", "."))

        # ============================================
        # 3) Si no hay precio, buscar en líneas siguientes
        #    1 o 2 líneas más abajo
        # ============================================
        if precio is None:
            for j in range(1, 3):
                if i + j < len(lines):
                    pm2 = precio_re.search(lines[i + j])
                    if pm2:
                        precio = float(pm2.group(1).replace(",", "."))
                        break

        # ============================================
        # 4) Si no hay formato pero parece receta vertical:
        #    Nombre en una línea, formato en la siguiente
        # ============================================
        if not formato and i + 1 < len(lines):
            if formato_re.match(lines[i + 1]):
                formato = lines[i + 1].strip()

        # ============================================
def parse_items_inteligente(text):
    # Normalizamos
    lines = [l.strip() for l in text.split("\n") if l.strip()]

    # Regex compartidos
    precio_re = re.compile(r"(\d+[.,]\d{1,2})\s*€?")
    # admite "Kg", "kg", "KG", "100gr", "30 UND", "Bandeja", "manojo"...
    formato_re = re.compile(
        r"\b((\d+\s*)?(gr|kg|KG|und|UND|uds|Uds|bandeja|Bandeja|manojo|Manojo))\b",
        re.IGNORECASE,
    )
    lambweston_re = re.compile(r"^[A-Z]{2}\d{3,}")

    # Palabras que nunca deben ser nombre de producto
    blacklist_nombres = {
        "FORMATO",
        "PRECIO",
        "PVP",
        "CÓDIGO",
        "CODIGO",
        "FRUTAS JAVIER CUEVAS S.L",
    }

    # ----------------------------------------------------------
    # 💡 DETECCIÓN AUTOMÁTICA DEL TIPO DE PÁGINA
    # ----------------------------------------------------------
    def es_tabla_horizontal(lines):
        columnas = 0
        for l in lines:
            partes = l.split()
            if len(partes) >= 3:
                columnas += 1
        return columnas > 6    # bastantes filas "anchas"

    def es_lambweston(lines):
        return any(lambweston_re.match(l) for l in lines)

    # ----------------------------------------------------------
    # 🔷 PARSER TIPO LAMBWESTON
    # ----------------------------------------------------------
    def parse_lambweston(lines):
        productos = []
        for i in range(len(lines)):
            l = lines[i]
            if lambweston_re.match(l):
                # código = l  # ahora mismo no lo usamos
                nombre = lines[i + 1] if i + 1 < len(lines) else ""
                formato = lines[i + 2] if i + 2 < len(lines) else ""

                # Buscar precio kg o caja en las 3 siguientes líneas
                precio_lineas = lines[i + 3 : i + 6]
                precio = None
                for pl in precio_lineas:
                    pm = precio_re.search(pl)
                    if pm:
                        precio = float(pm.group(1).replace(",", "."))
                        break

                if precio is None:
                    continue

                productos.append(
                    {
                        "nombre": nombre.replace('"', "").strip(),
                        "precio": precio,
                        "unidad_base": "unidad",
                        "cantidad_presentacion": 1,
                        "formato_presentacion": formato,
                        "iva_porcentaje": 10,
                        "merma": 0,
                    }
                )

        return productos

    # ----------------------------------------------------------
    # 🔶 PARSER TABLA HORIZONTAL GENÉRICO
    # ----------------------------------------------------------
    def parse_tabla_horizontal(lines):
        productos = []
        for l in lines:
            partes = [p.strip() for p in re.split(r"\s{2,}", l)]
            if len(partes) < 2:
                continue

            nombre = partes[0]
            precio = None
            formato = ""

            for p in partes:
                pm = precio_re.search(p)
                if pm:
                    precio = float(pm.group(1).replace(",", "."))
                if formato_re.search(p):
                    formato = p

            if precio is not None and nombre.upper() not in blacklist_nombres:
                productos.append(
                    {
                        "nombre": nombre,
                        "precio": precio,
                        "unidad_base": "unidad",
                        "cantidad_presentacion": 1,
                        "formato_presentacion": formato,
                        "iva_porcentaje": 10,
                        "merma": 0,
                    }
                )

        return productos

    # ----------------------------------------------------------
    # 🔸 PARSER VERTICAL EXTENDIDO
    #    (nombre + formato misma línea o línea siguiente,
    #     precio misma línea o una o dos líneas debajo)
    # ----------------------------------------------------------
    def parse_vertical_extendido(lines):
        productos = []
        i = 0

        while i < len(lines):
            linea = lines[i]

            partes = linea.split()
            nombre = None
            formato = None
            precio = None

            # 1) buscar formato en la MISMA línea (ej: "Granadas Kg")
            for p in partes:
                if formato_re.match(p):
                    formato = p
                    break

            if formato:
                nombre = linea.replace(formato, "").strip()
            else:
                # Si no hay formato en esta línea, el nombre es toda la línea
                nombre = linea

            # 2) precio en la MISMA línea
            pm = precio_re.search(linea)
            if pm:
                precio = float(pm.group(1).replace(",", "."))

            # 3) si no hay precio, buscar en las 2 siguientes líneas
            skip = 0
            if precio is None:
                for offset in range(1, 3):
                    if i + offset < len(lines):
                        pm2 = precio_re.search(lines[i + offset])
                        if pm2:
                            precio = float(pm2.group(1).replace(",", "."))
                            skip = offset
                            break

            # 4) si no hay formato y la siguiente línea ES formato
            if not formato and i + 1 < len(lines):
                if formato_re.match(lines[i + 1]):
                    formato = lines[i + 1].strip()
                    if skip == 0:
                        skip = 1

            # 5) limpieza: evitar basura tipo "Kg", "1.09€", "FORMATO", etc.
            nombre_limpio = (nombre or "").strip()
            if len(nombre_limpio) < 2:
                nombre_limpio = ""

            if nombre_limpio.upper() in blacklist_nombres:
                nombre_limpio = ""

            # 6) si tenemos nombre + precio → es un producto válido
            if nombre_limpio and precio is not None:
                productos.append(
                    {
                        "nombre": nombre_limpio,
                        "precio": precio,
                        "unidad_base": "unidad",
                        "cantidad_presentacion": 1,
                        "formato_presentacion": formato or "",
                        "iva_porcentaje": 10,
                        "merma": 0,
                    }
                )

                i += skip + 1  # saltar líneas usadas
            else:
                i += 1

        return productos

    # ----------------------------------------------------------
    # 🧠 DECISIÓN AUTOMÁTICA
    # ----------------------------------------------------------
    if es_lambweston(lines):
        return parse_lambweston(lines)

    if es_tabla_horizontal(lines):
        return parse_tabla_horizontal(lines)

    # Por defecto, tratamos la página como lista vertical
    return parse_vertical_extendido(lines)
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

        # Parsear items con el parser inteligente
        items = parse_items_inteligente(text)


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
