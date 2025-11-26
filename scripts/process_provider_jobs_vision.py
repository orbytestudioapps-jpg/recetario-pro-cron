import os
import requests
from google.cloud import vision
from supabase import create_client, Client
import re
import difflib
DEBUG_OCR = True   # Cambia a False en producción

# ================================
# 🔧 CONFIGURACIÓN
# ================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Google Vision Client
client_vision = vision.ImageAnnotatorClient()

# ================================
# 🔍 OCR GOOGLE VISION
# ================================
def ocr_google(url: str) -> str:
    resp = requests.get(url)
    content = resp.content

    image = vision.Image(content=content)
    response = client_vision.text_detection(image=image)

    if response.error.message:
        raise Exception(response.error.message)

    return response.text_annotations[0].description if response.text_annotations else ""

def log_debug(section: str, data=None):
    if not DEBUG_OCR:
        return

    print("\n" + "="*70)
    print(f"🔍 [DEBUG-OCR] {section}")
    print("="*70)

    if isinstance(data, list):
        for idx, item in enumerate(data):
            print(f"  [{idx:02}] {item}")
    else:
        print(data)

# ====================================================================
# 🧠 PARSER INTELIGENTE – AUTODETECCIÓN TIPOS DE LISTA
# ====================================================================
def parse_items_inteligente(text: str):
    """
    Parser OCR universal:
    - LambWeston (tablas americanas)
    - Tablas horizontales (CÓDIGO | NOMBRE | FORMATO | PVP)
    - Listas verticales (Javier Cuevas)
    """

    # Normalización OCR
    text = (
        text.replace("\t", " ")
            .replace("€", "")
            .replace("Kg.", "Kg")
            .replace("kg.", "kg")
            .replace("  ", " ")
            .strip()
    )
    lines = [l.strip() for l in text.split("\n") if l.strip()]

    log_debug("LÍNEAS TRAS NORMALIZACIÓN", lines[:50])  # primeras 50


    # Regex
    precio_re = re.compile(
        r"(?<!\d)"                  # Evita parte de números largos
        r"(\d{1,3}(?:[.,]\d{3})*"   # Miles: 1.000 / 1,000 / 12.000.000
        r"(?:[.,]\d{1,2})?"         # Decimales opcionales
        r")"
    )

    formato_re = re.compile(
        r"(?:\d+\s*(kg|g|gr|l|ml))|"
        r"(kg|g|gr|l|ml)|"
        r"(bandeja|bolsa|manojo|unidad|docena)|"
        r"(\d+\s*(bandeja|bolsa|manojo|unidad))",
        re.IGNORECASE
    )

    codigo_re = re.compile(r"^[A-Z]{2}\d{3}$")

    blacklist = {
        "FORMATO", "PRECIO", "PVP", "CÓDIGO", "CODIGO",
        "FRUTAS JAVIER CUEVAS", "SEMANA", "LISTADO", "EMAIL", "TELÉFONO"
    }

    def linea_valida(s):
        if len(s) < 3:
            return False
        if any(b in s.upper() for b in blacklist):
            return False
        return True

    lines = [l for l in lines if linea_valida(l)]

    # =====================================================
    # 🔷 DETECTAR LAMBWESTON (CÓDIGOS LWxxx)
    # =====================================================
    if any(codigo_re.match(l.split()[0]) for l in lines if len(l.split()) > 0):
        return parse_lambweston_ocr(lines, precio_re)

    # =====================================================
    # 🔶 DETECTAR TABLA HORIZONTAL (CÓDIGO / PVP)
    # =====================================================
    head = " ".join(lines[:15]).upper()
    if "CODIGO" in head and "PVP" in head:
        return parse_tabla_horizontal_ocr(lines, precio_re, formato_re)

    # =====================================================
    # 🔸 LISTA VERTICAL (Javier Cuevas)
    # =====================================================
    return parse_vertical_ocr(lines, precio_re, formato_re)

# ====================================================================
# 🔷 PARSER – LAMBWESTON
# ====================================================================
def parse_lambweston_ocr(lines, precio_re):
    productos = []

    for i in range(len(lines)):
        line = lines[i]
        log_debug("PROCESANDO LÍNEA", line)

        partes = line.split()
        if not partes:
            continue

        # Detectar código tipo LW054
        if not re.match(r"^[A-Z]{2}\d{3}$", partes[0]):
            continue

        codigo = partes[0]

        # Nombre en misma línea o siguiente
        nombre = " ".join(partes[1:]).strip()
        if not nombre and i + 1 < len(lines):
            nombre = lines[i + 1].strip()

        # Buscar formato (4 x 2.5 kg)
        formato = ""
        for j in range(i, min(i + 4, len(lines))):
            m = re.search(r"\d+\s*x\s*[\d.,]+\s*kg", lines[j].lower())
            if m:
                formato = m.group(0)
                break

        # Buscar precio
        precio = None
        for j in range(i, min(i + 7, len(lines))):
            m = precio_re.search(lines[j])
            if m:
                precio = float(m.group(1).replace(",", "."))
                break

        if precio is None:
            continue

        # EXTRACCIÓN DE FORMATO REAL
        cantidad, unidad, formato_final = extraer_cantidad_unidad(formato)

        # LIMPIEZA DE NOMBRE
        nombre_limpio = nombre.replace('"', "").strip()
        nombre_limpio = nombre_limpio.title()       # Capitalizar correctamente
        nombre_limpio = autocorregir_nombre(nombre_limpio)

        log_debug("EXTRACCIÓN", {
            "nombre": nombre_limpio,
            "precio": precio,
            "formato_raw": formato,
            "cantidad": cantidad,
            "unidad": unidad,
            "formato_final": formato_final,
        })

        productos.append({
            "nombre": nombre_limpio,
            "precio": precio,
            "unidad_base": unidad,
            "cantidad_presentacion": cantidad,
            "formato_presentacion": formato_final,
            "iva_porcentaje": 10,
            "merma": 0,
        })

    log_debug("PRODUCTOS DETECTADOS (LAMBWESTON)", productos)
    return productos

# ====================================================================
# 🔶 PARSER – TABLA HORIZONTAL (CÓDIGO | PRODUCTO | FORMATO | PVP)
# ====================================================================
def parse_tabla_horizontal_ocr(lines, precio_re, formato_re):
    productos = []

    for line in lines:
        log_debug("PROCESANDO LÍNEA", line)

        partes = [p.strip() for p in re.split(r"\s{2,}", line)]
        if len(partes) < 2:
            continue

        # Detectar si partes[0] es código (ej: LW054)
        es_codigo = bool(re.match(r"^[A-Za-z]{2}\d{3,}$", partes[0]))

        # Nombre según tipo de tabla
        nombre = partes[1] if es_codigo else partes[0]

        formato = ""
        precio = None

        for p in partes:
            if formato_re.search(p):
                formato = formato_re.search(p).group(0)
            if precio_re.search(p):
                precio = float(precio_re.search(p).group(1).replace(",", "."))

        if precio:
            cantidad, unidad, formato_final = extraer_cantidad_unidad(formato)

            nombre = nombre.replace(" unidad", "").replace(" Unidad", "")
            nombre = autocorregir_nombre(nombre)

            log_debug("EXTRACCIÓN", {
                "nombre": nombre,
                "precio": precio,
                "formato_raw": formato,
                "cantidad": cantidad,
                "unidad": unidad,
                "formato_final": formato_final,
            })

            productos.append({
                "nombre": nombre,
                "precio": precio,
                "unidad_base": unidad,
                "cantidad_presentacion": cantidad,
                "formato_presentacion": formato_final,
                "iva_porcentaje": 10,
                "merma": 0,
            })

    log_debug("PRODUCTOS DETECTADOS (TABLA HORIZONTAL)", productos)
    return productos

# ====================================================================
# 🔸 PARSER – LISTA VERTICAL (Javier Cuevas)
# ====================================================================
def parse_vertical_ocr(lines, precio_re, formato_re):
    productos = []
    i = 0

    while i < len(lines):
        line = lines[i]
        log_debug("PROCESANDO LÍNEA", line)

        # ⬆ NUEVO: ignorar líneas que son SOLO unidades
        UNIDADES_SOLO = {
            "kg", "kg.", "g", "gr", "ml", "l",
            "manojo", "bandeja", "bolsa", "unidad"
        }

        if line.lower() in UNIDADES_SOLO:
            i += 1
            continue

        # Buscar precio en línea o siguientes
        pm = precio_re.search(line)
        precio = None
        skip = 0

        if pm:
            precio = float(pm.group(1).replace(",", "."))
        else:
            for j in range(1, 3):
                if i + j < len(lines):
                    pm2 = precio_re.search(lines[i + j])
                    if pm2:
                        precio = float(pm2.group(1).replace(",", "."))
                        skip = j
                        break

        if precio is None:
            i += 1
            continue

        # Buscar formato (kg, gr, bandeja, etc.) en esta línea o las siguientes
        fm = formato_re.search(line)
        formato = fm.group(0) if fm else ""

        # ⬆ NUEVO: Si no hay formato, mirar línea ANTERIOR (muy importante)
        if not formato and i - 1 >= 0:
            fm_prev = formato_re.search(lines[i - 1])
            if fm_prev:
                formato = fm_prev.group(0)

        # ⬆ NUEVO: Si no está arriba, usar tu lógica original (próximas 2 líneas)
        if not formato:
            for j in range(1, 3):
                if i + j < len(lines):
                    fm2 = formato_re.search(lines[i + j])
                    if fm2:
                        formato = fm2.group(0)
                        break

        # Extraer cantidad/unidad antes de limpiar nombre
        cantidad, unidad, formato_final = extraer_cantidad_unidad(formato)

        # Nombre limpio:
        nombre = line

        if formato:
            nombre = nombre.replace(formato, "")

        if pm:
            nombre = nombre.replace(pm.group(1), "")

        nombre = (
            nombre.replace("  ", " ")
                  .strip(" -.").strip()
        )

        # ⬆ NUEVO: Si el nombre quedó vacío, usar línea anterior SOLO si no es una unidad
        if (not nombre or len(nombre) <= 2) and i - 1 >= 0:
            prev = lines[i - 1].strip().lower()
            if prev not in UNIDADES_SOLO and not precio_re.search(prev):
                nombre = lines[i - 1].strip()

        nombre = autocorregir_nombre(nombre)

        # 🔍 DEBUG de extracción
        log_debug("EXTRACCIÓN", {
            "nombre": nombre,
            "precio": precio,
            "formato_raw": formato,
            "cantidad": cantidad,
            "unidad": unidad,
            "formato_final": formato_final,
        })

        # Validación nombre
        if nombre and len(nombre) > 2:
            productos.append({
                "nombre": nombre,
                "precio": precio,
                "unidad_base": unidad,
                "cantidad_presentacion": cantidad,
                "formato_presentacion": formato_final,
                "iva_porcentaje": 10,
                "merma": 0,
            })

        i += skip + 1

    log_debug("PRODUCTOS DETECTADOS (VERTICAL)", productos)
    return productos
    
def normalizar_formato(formato_raw: str):
    if not formato_raw:
        return ""

    f = formato_raw.strip().lower()

    # Eliminar caracteres sueltos que OCR confunde como unidad
    if f in {"l", "g"}:
        return ""

    # Detectar número + unidad
    m = re.match(r"(\d+)\s*(kg|g|gr|l|ml)$", f)
    if m:
        return m.group(2)  # devolver unidad normalizada

    # Detectar unidades sueltas válidas
    unidades_validas = {
        "kg", "g", "gr", "ml", "l",
        "manojo", "bandeja", "bolsa", "unidad", "docena"
    }

    for u in unidades_validas:
        if u in f:
            return u

    return ""

def extraer_cantidad_unidad(formato_raw: str):
    """
    Extrae cantidad numérica y unidad real desde cadenas tipo:
    - '125gr'
    - '400 gr'
    - '2 kg'
    - '4 x 2.5 kg'
    - 'bandeja 125gr'
    """

    if not formato_raw:
        return 1, "unidad", ""  # por defecto

    f = formato_raw.lower().strip()

    # Caso 1: formato tipo "4 x 2.5 kg"
    m = re.match(r"(\d+)\s*x\s*([\d.,]+)\s*(kg|g|gr|l|ml)", f)
    if m:
        multiplicador = int(m.group(1))
        cantidad = float(m.group(2).replace(",", "."))
        unidad = m.group(3)
        return cantidad, unidad, f  # formato completo

    # Caso 2: "125gr", "500gr", "200 g"
    m = re.match(r"([\d.,]+)\s*(kg|g|gr|l|ml)$", f)
    if m:
        cantidad = float(m.group(1).replace(",", "."))
        unidad = m.group(2)
        return cantidad, unidad, ""  # sin formato extra

    # Caso 3: formato suelto sin cantidad
    unidades_validas = {"kg", "g", "gr", "l", "ml", "unidad"}
    for u in unidades_validas:
        if f == u:
            return 1, u, ""

    # Caso 4: mezcla tipo "bandeja 125gr"
    m = re.search(r"([\d.,]+)\s*(kg|g|gr|l|ml)", f)
    if m:
        cantidad = float(m.group(1).replace(",", "."))
        unidad = m.group(2)
        return cantidad, unidad, f

    # Nada encontrado
    return 1, "unidad", ""

# Diccionario básico de productos (puedes ampliarlo)
DICCIONARIO_PRODUCTOS = [
    "Tomate", "Tomate pera", "Tomate cherry",
    "Pimiento", "Pimiento verde", "Pimiento rojo", "Pimiento amarillo",
    "Cebolla", "Cebolla morada", "Cebolleta",
    "Patata", "Batata",
    "Melocotón", "Melón", "Sandía",
    "Aguacate", "Pak choi", "Apio", "Ajo", "Ajetes",
    "Mezclum", "Rúcula", "Berros", "Lechuga",
    "Cilantro", "Perejil", "Eneldo", "Tomillo", "Romero", "Orégano",
    "Albahaca", "Hierbabuena", "Menta",
    "Champiñón", "Setas", "Portobello",
    "Zanahoria", "Pepino", "Calabacín", "Berenjena",
    "Nueces", "Almendras", "Sésamo", "Curry", "Pimentón", "Granadas", "Tagete"
]

def autocorregir_nombre(nombre: str) -> str:
    n = nombre.strip()
    n = n.title()  # Normalizar capitalización

    # Correcciones OCR comunes
    reemplazos = {
        "meocotón": "Melocotón",
        "meon": "Melón",
        "meón": "Melón",
        "ciantro": "Cilantro",
        "ciant": "Cilantro",
        "ceboeta": "Cebolleta",
        "uvas aedo": "Uvas",
        "pátano": "Plátano",
        "ranadas": "Granadas",
        "for": "Flor",
        "fora": "Flor",
        "omabarda": "Lombarda",
        "ombarda": "Lombarda",
        "pensamimento": "Pensamiento",
        "pensaminto": "Pensamiento",
        "uarnición": "Guarnición",
    }

    for k, v in reemplazos.items():
        if k in n.lower():
            return v

    # Fuzzy matching extendido
    dicc_ext = DICCIONARIO_PRODUCTOS + [p.lower() for p in DICCIONARIO_PRODUCTOS]

    mejor = difflib.get_close_matches(n.lower(), dicc_ext, n=1, cutoff=0.68)
    if mejor:
        return mejor[0].title()

    return n

# ====================================================================
# 🔄 PROCESAR UN JOB INDIVIDUAL
# ====================================================================
def process_job(job):
    print(f"\n==============================")
    print(f"🟦 Procesando página {job['numero_pagina']} – {job['archivo_url']}")
    print("==============================\n")

    supabase.table("proveedor_listas_jobs") \
        .update({"estado": "procesando"}) \
        .eq("id", job["id"]).execute()

    try:
        # OCR
        text = ocr_google(job["archivo_url"])

        log_debug("RAW OCR TEXT", text)
        log_debug("RAW OCR LINES", text.split("\n"))


        # Parse
        items = parse_items_inteligente(text)

        print(f"✔ Detectados {len(items)} productos.")

        # Insertar uno por uno
        for item in items:
            item["proveedor_id"] = job["proveedor_id"]
            item["organizacion_id"] = job["organizacion_id"]
            item["creado_desde_archivo"] = job["lista_id"]
            item["pagina"] = job["numero_pagina"]

            print(f"➡️ INSERT → {item['nombre']}")

            supabase.table("proveedor_listas_items").insert(item).execute()

        # Marcar job como completado
        supabase.table("proveedor_listas_jobs") \
            .update({"estado": "procesado"}) \
            .eq("id", job["id"]).execute()

        print("✅ Página procesada correctamente.")

    except Exception as e:
        print("❌ ERROR:", e)
        supabase.table("proveedor_listas_jobs") \
            .update({"estado": "error", "error": str(e)}) \
            .eq("id", job["id"]).execute()

# ====================================================================
# 📊 ACTUALIZAR PROGRESO
# ====================================================================
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
        .eq("id", lista_id).execute()

    print(f"📦 {procesados}/{total} lotes — Estado: {estado}")

# ====================================================================
# ▶ MAIN
# ====================================================================
def main():
    jobs = supabase.table("proveedor_listas_jobs") \
        .select("*") \
        .eq("estado", "pendiente") \
        .order("numero_pagina", desc=False) \
        .execute().data

    if not jobs:
        print("No pending jobs.")
        return

    print(f"🔍 {len(jobs)} jobs pendientes.\n")

    for job in jobs:
        process_job(job)
        actualizar_progreso(job["lista_id"])

    print("\n✔ OCR COMPLETADO.")

if __name__ == "__main__":
    main()
