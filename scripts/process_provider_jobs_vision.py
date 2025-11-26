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
# 📌 Parser inteligente
# ================================
def parse_items_inteligente(text: str):
    # Normalizamos
    lines = [l.strip() for l in text.split("\n") if l.strip()]

    # Regex compartidos
    precio_re = re.compile(r"(\d+[.,]\d{1,2})\s*€?")
    formato_re = re.compile(
        r"\b((\d+\s*)?(gr|kg|KG|und|UND|uds|Uds|bandeja|Bandeja|manojo|Manojo|cart[oó]n|Cart[oó]n|docena))\b",
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

    def solo_precio(s: str) -> bool:
        # true si la línea es básicamente un precio tipo "1.09€"
        return bool(precio_re.fullmatch(s.replace(" ", "")))

    def es_unidad_suelta(s: str) -> bool:
        s2 = s.strip().lower()
        return s2 in {"kg", "kg.", "und", "uds", "unidad", "bandeja", "manojo"}

    # ----------------------------------------------------------
    # 💡 DETECCIÓN AUTOMÁTICA DEL TIPO DE PÁGINA
    # ----------------------------------------------------------
    def es_lambweston(lines_local):
        return any(lambweston_re.match(l) for l in lines_local)

    # Tabla genérica “seria”: tiene cabecera CÓDIGO + PVP
    def es_tabla_generica(lines_local):
        head = " ".join(lines_local[:10]).upper()
        return ("CODIGO" in head or "CÓDIGO" in head) and "PVP" in head

    # ----------------------------------------------------------
    # 🔷 PARSER TIPO LAMBWESTON
    # ----------------------------------------------------------
    def parse_lambweston(lines_local):
        productos = []
        for i, l in enumerate(lines_local):
            if lambweston_re.match(l):
                # código = l   # si algún día quieres guardarlo
                nombre = lines_local[i + 1] if i + 1 < len(lines_local) else ""
                formato = lines_local[i + 2] if i + 2 < len(lines_local) else ""

                # Buscar precio kg / caja en las siguientes líneas
                precio = None
                for pl in lines_local[i + 3 : i + 7]:
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
    # 🔶 PARSER TABLA HORIZONTAL GENÉRICO (para tablas con CÓDIGO / PVP)
    # ----------------------------------------------------------
    def parse_tabla_horizontal(lines_local):
        productos = []
        for l in lines_local:
            partes = [p.strip() for p in re.split(r"\s{2,}", l)]
            if len(partes) < 2:
                continue

            # En tablas suele ser: CÓDIGO | NOMBRE | FORMATO | PVP
            # usamos la segunda columna como nombre si existe
            nombre = partes[1] if len(partes) > 1 else partes[0]
            precio = None
            formato = ""

            for p in partes:
                pm = precio_re.search(p)
                if pm:
                    precio = float(pm.group(1).replace(",", "."))
                if formato_re.search(p):
                    formato = p

            if (
                precio is not None
                and nombre.upper() not in blacklist_nombres
                and not solo_precio(nombre)
                and not es_unidad_suelta(nombre)
            ):
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
    # 🔸 PARSER VERTICAL EXTENDIDO (para TODO lo Javier Cuevas)
    # ----------------------------------------------------------
    def parse_vertical_extendido(lines_local):
        productos = []
        i = 0

        while i < len(lines_local):
            linea = lines_local[i]

            partes = linea.split()
            nombre = None
            formato = None
            precio = None

            # 1) formato en la MISMA línea (ej: "Granadas Kg")
            for p in partes:
                if formato_re.match(p):
                    formato = p
                    break

            if formato:
                nombre = linea.replace(formato, "").strip()
            else:
                nombre = linea

            # 2) precio en la MISMA línea
            pm = precio_re.search(linea)
            if pm:
                precio = float(pm.group(1).replace(",", "."))

            # 3) si no hay precio, buscar en 2 líneas siguientes
            skip = 0
            if precio is None:
                for offset in range(1, 3):
                    if i + offset < len(lines_local):
                        pm2 = precio_re.search(lines_local[i + offset])
                        if pm2:
                            precio = float(pm2.group(1).replace(",", "."))
                            skip = offset
                            break

            # 4) si no hay formato, mirar línea siguiente
            if not formato and i + 1 < len(lines_local):
                if formato_re.match(lines_local[i + 1]):
                    formato = lines_local[i + 1].strip()
                    if skip == 0:
                        skip = 1

            # 5) limpieza de nombre
            nombre_limpio = (nombre or "").strip()

            if len(nombre_limpio) <= 2:
                nombre_limpio = ""

            if nombre_limpio.upper() in blacklist_nombres:
                nombre_limpio = ""

            if solo_precio(nombre_limpio):
                nombre_limpio = ""

            if es_unidad_suelta(nombre_limpio):
                nombre_limpio = ""

            if not re.search(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]", nombre_limpio):
                nombre_limpio = ""

            # 6) producto válido
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
                i += skip + 1
            else:
                i += 1

        return productos

    # ----------------------------------------------------------
    # 🧠 DECISIÓN AUTOMÁTICA
    # ----------------------------------------------------------
    if es_lambweston(lines):
        # Tablas tipo Lambweston
        return parse_lambweston(lines)

    if es_tabla_generica(lines):
        # Otras tablas con cabecera CÓDIGO / PVP
        return parse_tabla_horizontal(lines)

    # ✅ Cualquier otra cosa → listas verticales (tus Javier Cuevas)
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
    main()        "PRECIO",
        "PVP",
        "CÓDIGO",
        "CODIGO",
        "FRUTAS JAVIER CUEVAS S.L",
    }

    def solo_precio(s: str) -> bool:
        # true si la línea es básicamente un precio tipo "1.09€"
        return bool(precio_re.fullmatch(s.replace(" ", "")))

    def es_unidad_suelta(s: str) -> bool:
        s2 = s.strip().lower()
        return s2 in {"kg", "kg.", "und", "uds", "unidad", "bandeja", "manojo"}

    # ----------------------------------------------------------
    # 💡 DETECCIÓN AUTOMÁTICA DEL TIPO DE PÁGINA
    # ----------------------------------------------------------
    def es_lambweston(lines_local):
        return any(lambweston_re.match(l) for l in lines_local)

    # Tabla genérica “seria”: tiene cabecera CÓDIGO + PVP
    def es_tabla_generica(lines_local):
        head = " ".join(lines_local[:10]).upper()
        return ("CODIGO" in head or "CÓDIGO" in head) and "PVP" in head

    # ----------------------------------------------------------
    # 🔷 PARSER TIPO LAMBWESTON
    # ----------------------------------------------------------
    def parse_lambweston(lines_local):
        productos = []
        for i, l in enumerate(lines_local):
            if lambweston_re.match(l):
                # código = l   # si algún día quieres guardarlo
                nombre = lines_local[i + 1] if i + 1 < len(lines_local) else ""
                formato = lines_local[i + 2] if i + 2 < len(lines_local) else ""

                # Buscar precio kg / caja en las siguientes líneas
                precio = None
                for pl in lines_local[i + 3 : i + 7]:
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
    # 🔶 PARSER TABLA HORIZONTAL GENÉRICO (para tablas con CÓDIGO / PVP)
    # ----------------------------------------------------------
    def parse_tabla_horizontal(lines_local):
        productos = []
        for l in lines_local:
            partes = [p.strip() for p in re.split(r"\s{2,}", l)]
            if len(partes) < 2:
                continue

            # En tablas suele ser: CÓDIGO | NOMBRE | FORMATO | PVP
            # usamos la segunda columna como nombre si existe
            nombre = partes[1] if len(partes) > 1 else partes[0]
            precio = None
            formato = ""

            for p in partes:
                pm = precio_re.search(p)
                if pm:
                    precio = float(pm.group(1).replace(",", "."))
                if formato_re.search(p):
                    formato = p

            if (
                precio is not None
                and nombre.upper() not in blacklist_nombres
                and not solo_precio(nombre)
                and not es_unidad_suelta(nombre)
            ):
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
    # 🔸 PARSER VERTICAL EXTENDIDO (para TODO lo Javier Cuevas)
    # ----------------------------------------------------------
    def parse_vertical_extendido(lines_local):
        productos = []
        i = 0

        while i < len(lines_local):
            linea = lines_local[i]

            partes = linea.split()
            nombre = None
            formato = None
            precio = None

            # 1) formato en la MISMA línea (ej: "Granadas Kg")
            for p in partes:
                if formato_re.match(p):
                    formato = p
                    break

            if formato:
                nombre = linea.replace(formato, "").strip()
            else:
                nombre = linea

            # 2) precio en la MISMA línea
            pm = precio_re.search(linea)
            if pm:
                precio = float(pm.group(1).replace(",", "."))

            # 3) si no hay precio, buscar en 2 líneas siguientes
            skip = 0
            if precio is None:
                for offset in range(1, 3):
                    if i + offset < len(lines_local):
                        pm2 = precio_re.search(lines_local[i + offset])
                        if pm2:
                            precio = float(pm2.group(1).replace(",", "."))
                            skip = offset
                            break

            # 4) si no hay formato, mirar línea siguiente
            if not formato and i + 1 < len(lines_local):
                if formato_re.match(lines_local[i + 1]):
                    formato = lines_local[i + 1].strip()
                    if skip == 0:
                        skip = 1

            # 5) limpieza de nombre
            nombre_limpio = (nombre or "").strip()

            if len(nombre_limpio) <= 2:
                nombre_limpio = ""

            if nombre_limpio.upper() in blacklist_nombres:
                nombre_limpio = ""

            if solo_precio(nombre_limpio):
                nombre_limpio = ""

            if es_unidad_suelta(nombre_limpio):
                nombre_limpio = ""

            if not re.search(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]", nombre_limpio):
                nombre_limpio = ""

            # 6) producto válido
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
                i += skip + 1
            else:
                i += 1

        return productos

    # ----------------------------------------------------------
    # 🧠 DECISIÓN AUTOMÁTICA
    # ----------------------------------------------------------
    if es_lambweston(lines):
        # Tablas tipo Lambweston
        return parse_lambweston(lines)

    if es_tabla_generica(lines):
        # Otras tablas con cabecera CÓDIGO / PVP
        return parse_tabla_horizontal(lines)

    # ✅ Cualquier otra cosa → listas verticales (tus Javier Cuevas)
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
    """
    Actualiza el campo 'progreso' en proveedor_listas
    contando los items que ya tienen nombre + precio.
    """
    # Obtener todos los items procesados
    data = (
        supabase
        .table("proveedor_listas_items")
        .select("id")
        .eq("lista_id", lista_id)
        .not_.is_("nombre", None)
        .execute()
    )

    procesados = len(data.data) if data.data else 0

    # Actualizar progreso en proveedor_listas
    supabase.table("proveedor_listas").update({
        "progreso": procesados
    }).eq("id", lista_id).execute()

    print(f"🔄 Progreso actualizado: {procesados}")

def parse_items_inteligente(text: str):
    """
    Analiza texto OCR de listas de proveedores y extrae:
    - nombre del producto
    - formato / unidad (kg, bandeja, bolsa...)
    - precio (€)
    Compatible con listas como las de Frutas Javier Cuevas.
    """

    # Normalización general del texto OCR
    text = (
        text.replace("\t", " ")
            .replace("€", "")
            .replace("..", ".")
            .replace(" ,", ",")
            .replace("  ", " ")
            .strip()
    )

    # Dividir líneas no vacías
    lines = [l.strip() for l in text.split("\n") if l.strip()]

    # Regex compatibles con OCR
    precio_re = re.compile(
        r"(?<!\d)(\d+[.,]\d{1,2})(?:\s*(€|eur|euro))?",
        re.IGNORECASE
    )

    formato_re = re.compile(
        r"(\d+\s*(kg|g|gr|l|ml)|"
        r"\bkg\b|\bg\b|\bgr\b|\bl\b|\bml\b|"
        r"\b(bandeja|bolsa|manojo|unidad|docena)\b|"
        r"\d+\s*(bandeja|bolsa|manojo|unidad))",
        re.IGNORECASE
    )

    # Filtrar líneas basura del OCR
    blacklist = {
        "FORMATO", "PRECIO", "PVP", "CODIGO",
        "FRUTAS JAVIER CUEVAS", "TELÉFONO", "EMAIL",
        "SEMANA", "LISTADO", "COTIZACION"
    }

    def linea_basura(s):
        s_up = s.upper()
        return any(b in s_up for b in blacklist)

    def solo_precio(s):
        s2 = s.replace(" ", "")
        return bool(precio_re.fullmatch(s2))

    items = []

    for line in lines:
        original = line

        if linea_basura(line):
            continue
        if solo_precio(line):
            continue
        if len(line) < 4:
            continue

        # Buscar precio (usualmente al final)
        precio_match = precio_re.search(line)
        if not precio_match:
            continue  # Línea inválida, no contiene precio

        precio_str = precio_match.group(1).replace(",", ".")
        try:
            precio = float(precio_str)
        except:
            continue

        # Quitar precio de la línea para identificar producto
        line_sin_precio = line[:precio_match.start()].strip()

        # Buscar formato o unidad
        formato_match = formato_re.search(line_sin_precio)
        formato = formato_match.group(0).strip() if formato_match else ""

        # Extraer nombre
        if formato:
            nombre = line_sin_precio.replace(formato, "").strip()
        else:
            nombre = line_sin_precio.strip()

        # Limpiar nombre
        nombre = re.sub(r"\s{2,}", " ", nombre)
        nombre = nombre.strip(" -.")

        if not nombre:
            continue

        # Registrar item final
        items.append({
            "nombre": nombre,
            "formato": formato,
            "precio": precio,
            "raw": original
        })

    return items

    # ----------------------------------------------------------
    # 💡 DETECCIÓN AUTOMÁTICA DEL TIPO DE PÁGINA
    # ----------------------------------------------------------
    def es_lambweston(lines):
        return any(lambweston_re.match(l) for l in lines)

    # Tabla genérica “seria”: tiene cabecera CÓDIGO + PVP
    def es_tabla_generica(lines):
        head = " ".join(lines[:10]).upper()
        return ("CODIGO" in head or "CÓDIGO" in head) and "PVP" in head

    # ----------------------------------------------------------
    # 🔷 PARSER TIPO LAMBWESTON
    # ----------------------------------------------------------
    def parse_lambweston(lines):
        productos = []
        for i in range(len(lines)):
            l = lines[i]
            if lambweston_re.match(l):
                # código = l   # si algún día quieres guardarlo
                nombre = lines[i + 1] if i + 1 < len(lines) else ""
                formato = lines[i + 2] if i + 2 < len(lines) else ""

                # Buscar precio kg / caja en las siguientes líneas
                precio = None
                for pl in lines[i + 3 : i + 7]:
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
    # 🔶 PARSER TABLA HORIZONTAL GENÉRICO (para tablas con CÓDIGO / PVP)
    # ----------------------------------------------------------
    def parse_tabla_horizontal(lines):
        productos = []
        for l in lines:
            partes = [p.strip() for p in re.split(r"\s{2,}", l)]
            if len(partes) < 2:
                continue

            nombre = partes[1] if len(partes) > 1 else partes[0]
            precio = None
            formato = ""

            for p in partes:
                pm = precio_re.search(p)
                if pm:
                    precio = float(pm.group(1).replace(",", "."))
                if formato_re.search(p):
                    formato = p

            if (
                precio is not None
                and nombre.upper() not in blacklist_nombres
                and not solo_precio(nombre)
                and not es_unidad_suelta(nombre)
            ):
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
    # 🔸 PARSER VERTICAL EXTENDIDO (para TODO lo Javier Cuevas)
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

            # 1) formato en la MISMA línea (ej: "Granadas Kg")
            for p in partes:
                if formato_re.match(p):
                    formato = p
                    break

            if formato:
                nombre = linea.replace(formato, "").strip()
            else:
                nombre = linea

            # 2) precio en la MISMA línea
            pm = precio_re.search(linea)
            if pm:
                precio = float(pm.group(1).replace(",", "."))

            # 3) si no hay precio, buscar en 2 líneas siguientes
            skip = 0
            if precio is None:
                for offset in range(1, 3):
                    if i + offset < len(lines):
                        pm2 = precio_re.search(lines[i + offset])
                        if pm2:
                            precio = float(pm2.group(1).replace(",", "."))
                            skip = offset
                            break

            # 4) si no hay formato, mirar línea siguiente
            if not formato and i + 1 < len(lines):
                if formato_re.match(lines[i + 1]):
                    formato = lines[i + 1].strip()
                    if skip == 0:
                        skip = 1

            # 5) limpieza de nombre
            nombre_limpio = (nombre or "").strip()

            if len(nombre_limpio) <= 2:
                nombre_limpio = ""

            if nombre_limpio.upper() in blacklist_nombres:
                nombre_limpio = ""

            if solo_precio(nombre_limpio):
                nombre_limpio = ""

            if es_unidad_suelta(nombre_limpio):
                nombre_limpio = ""

            if not re.search(r"[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]", nombre_limpio):
                nombre_limpio = ""

            # 6) producto válido
            if nombre_limpio and precio is not None:
                productos.append({
                    "nombre": nombre_limpio,
                    "precio": precio,
                    "unidad_base": "unidad",
                    "cantidad_presentacion": 1,
                    "formato_presentacion": formato or "",
                    "iva_porcentaje": 10,
                    "merma": 0,
                })
                i += skip + 1
            else:
                i += 1

        return productos

    # ----------------------------------------------------------
    # 🧠 DECISIÓN AUTOMÁTICA
    # ----------------------------------------------------------
    if es_lambweston(lines):
        # Tablas tipo Lambweston (COMIDA AMERICANA)
        return parse_lambweston(lines)

    if es_tabla_generica(lines):
        # Otras tablas con cabecera CÓDIGO / PVP
        return parse_tabla_horizontal(lines)

    # ✅ Cualquier otra cosa → listas verticales (tus páginas 1-9 de Javier Cuevas)
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
