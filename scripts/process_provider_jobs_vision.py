import os
import requests
from google.cloud import vision
from supabase import create_client, Client
import re
import difflib

DEBUG_OCR = True   # Cambiar a False en prod

# ======================================================
# 🔧 CONFIGURACIÓN
# ======================================================
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

client_vision = vision.ImageAnnotatorClient()


# ======================================================
# 🧹 STOPWORDS Y FILTRO DE LÍNEAS BASURA
# ======================================================
STOPWORDS = [
    "% iva",
    "importe iva",
    "nombre:",
    "cajas:",
    "lineas:",
    "líneas:",
    "suma",
    "registro sanitario",
    "no se admiten",
    "total cajas",
    "frutas javier cuevas",
    "javier cuevas",
    "no se admiten devoluciones",
    "horas desde la entrega",
]

def linea_es_basura(texto: str) -> bool:
    t = texto.lower().strip()
    if len(t) < 2:
        return True
    return any(sw in t for sw in STOPWORDS)


# ======================================================
# 📏 NORMALIZAR UNIDADES (kg, g, L, mL)
# ======================================================
def normalizarUnidad(u: str):
    if not u:
        return u

    t = u.lower().strip()

    if t in ["kg", "kilo", "kilos", "kgs", "kg."]:
        return "kg"

    if t in ["g", "gr", "grs", "gramos", "gr.", "g."]:
        return "g"

    if t in ["l", "lt", "litro", "litros", "l."]:
        return "L"

    if t in ["ml", "mililitro", "mililitros", "ml."]:
        return "mL"

    return u


# ======================================================
# 🔍 OCR GOOGLE VISION
# ======================================================
def ocr_google(url: str) -> str:
    resp = requests.get(url)
    content = resp.content
    image = vision.Image(content=content)
    response = client_vision.text_detection(image=image)
    if response.error.message:
        raise Exception(response.error.message)

    return response.text_annotations[0].description if response.text_annotations else ""


# ======================================================
# 🔍 DEBUG
# ======================================================
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


# ======================================================
# 🚀 PARSER UNIVERSAL DE TEXTOS OCR
# ======================================================
def parse_items_inteligente(text: str):
    text = (
        text.replace("\t", " ")
            .replace("€", "")
            .replace("  ", " ")
            .strip()
    )

    lines = [l.strip() for l in text.split("\n") if l.strip()]

    log_debug("LÍNEAS TRAS NORMALIZACIÓN (RAW)", lines[:80])

    # -------------------------------
    # PATTERNS CORREGIDOS (NO ROMPEN PALABRAS)
    # -------------------------------
    precio_re = re.compile(
        r"\b(\d{1,4}(?:[.,]\d{1,3})?(?:[.,]\d{1,2})?)\b"   # 7,500 → 7.50
    )

    formato_re = re.compile(
        r"\b\d+[.,]?\d*\s*(kg|g|gr|l|ml)\b|"               # 125gr, 5 kg
        r"\b(kg|g|gr|l|ml)\b|"                             # unidades sueltas
        r"\b(bandeja|bolsa|manojo|unidad|docena)\b",
        re.IGNORECASE
    )

    # códigos tipo AB12345
    codigo_re = re.compile(r"^[A-Za-z]{1,3}\d{2,6}$")

    # -----------------------------
    # LIMPIAR LÍNEAS BASURA
    # -----------------------------
    blacklist = {
        "FORMATO", "PRECIO", "PVP",
        "CÓDIGO", "CODIGO",
        "CLIENTE", "FACTURA", "N.I.F",
        "TELÉFONO", "FECHA", "ALBARÁN",
        "Lote", "Cad.", "CAD."
    }

    def linea_valida(s):
        if len(s) < 2:
            return False
        if any(b in s.upper() for b in blacklist):
            return False
        if linea_es_basura(s):
            return False
        return True

    lines = [l for l in lines if linea_valida(l)]

    log_debug("LÍNEAS TRAS FILTRO BASURA", lines[:80])

    # -----------------------------
    # DETECCIÓN DE TIPO DE DOCUMENTO
    # -----------------------------

    # ✔️ 1) FACTURA (Código – Descripción – Kilos – Precio – Importe)
    if detectar_factura(lines):
        return parse_factura(lines, precio_re)

    # ✔️ 2) LAMBWESTON
    if any(re.match(r"^[A-Z]{2}\d{3,}", l.split()[0]) for l in lines if len(l.split()) > 0):
        return parse_lambweston(lines, precio_re)

    # ✔️ 3) TABLA HORIZONTAL CON CÓDIGO
    if any("PVP" in l.upper() or "FORMATO" in l.upper() for l in lines[:10]):
        return parse_tabla_horizontal(lines, precio_re, formato_re)

    # ✔️ 4) LISTA VERTICAL (Javier Cuevas)
    return parse_vertical(lines, precio_re, formato_re)


# ======================================================
# 🧾 DETECTAR FACTURA
# ======================================================
def detectar_factura(lines):
    """
    Detecta estructuras tipo FACTURA:
    CÓDIGO | DESCRIPCIÓN | KILOS | PRECIO | IMPORTE
    """
    hits = 0
    for l in lines:
        partes = l.split()
        # Detectar fila con: código + kilos + precio + importe
        if len(partes) >= 5:
            if re.match(r"^\d{2,5}$", partes[0]):  # 226 / 4045 / etc
                if re.search(r"\d+[.,]\d{1,3}", l):  # kilos
                    if re.search(r"\d+[.,]\d{1,2}", l):  # precios
                        hits += 1

    return hits >= 2  # mínimo dos líneas válidas


# ======================================================
# 🧾 PARSER DE FACTURAS
# ======================================================
def parse_factura(lines, precio_re):
    productos = []

    for l in lines:
        partes = l.split()
        if len(partes) < 5:
            continue

        # código
        if not re.match(r"^\d{2,5}$", partes[0]):
            continue

        codigo = partes[0]

        # extract números
        numeros = precio_re.findall(l)
        if len(numeros) < 2:
            continue

        kilos = float(numeros[0].replace(",", "."))
        precio = float(numeros[1].replace(",", "."))

        # descripción está entre el código y los números
        desc = l.replace(codigo, "")
        desc = desc.replace(str(numeros[0]), "")
        desc = desc.replace(str(numeros[1]), "")
        desc = desc.strip(" -.")

        desc = normalizarNombre(desc)

        productos.append({
            "nombre": desc,
            "precio": precio,
            "unidad_base": normalizarUnidad("kg"),
            "cantidad_presentacion": kilos,
            "formato_presentacion": f"{kilos} kg",
            "iva_porcentaje": 10,
            "merma": 0,
        })

    log_debug("PRODUCTOS FACTURA", productos)
    return productos


# ======================================================
# LAMBWESTON
# ======================================================
def parse_lambweston(lines, precio_re):
    productos = []
    for i in range(len(lines)):
        l = lines[i]
        partes = l.split()
        if not partes:
            continue

        if not re.match(r"^[A-Z]{2}\d{3,}", partes[0]):
            continue

        codigo = partes[0]
        nombre = " ".join(partes[1:]) if len(partes) > 1 else ""
        if not nombre and i + 1 < len(lines):
            nombre = lines[i+1]

        precio = None
        for j in range(i, min(i+5, len(lines))):
            pm = precio_re.search(lines[j])
            if pm:
                precio = float(pm.group(1).replace(",", "."))
                break

        if precio is None:
            continue

        nombre = normalizarNombre(nombre)

        productos.append({
            "nombre": nombre,
            "precio": precio,
            "unidad_base": "unidad",
            "cantidad_presentacion": 1,
            "formato_presentacion": "",
            "iva_porcentaje": 10,
            "merma": 0,
        })

    log_debug("PRODUCTOS LAMBWESTON", productos)
    return productos


# ======================================================
# TABLA HORIZONTAL
# ======================================================
def parse_tabla_horizontal(lines, precio_re, formato_re):
    productos = []

    for l in lines:
        if linea_es_basura(l):
            continue

        partes = [p.strip() for p in re.split(r"\s{2,}", l)]
        if len(partes) < 2:
            continue

        precio = None
        formato = ""

        for p in partes:
            if precio_re.search(p):
                precio = float(precio_re.search(p).group(1).replace(",", "."))
            if formato_re.search(p):
                formato = formato_re.search(p).group(0)

        if precio is None:
            continue

        nombre = partes[0]
        nombre = normalizarNombre(nombre)

        # unidad base a partir del formato (si aplica)
        unidad_base = "unidad"
        if formato:
            um = re.search(r"(kg|g|gr|l|ml)", formato, re.IGNORECASE)
            if um:
                unidad_base = normalizarUnidad(um.group(1))

        productos.append({
            "nombre": nombre,
            "precio": precio,
            "unidad_base": unidad_base,
            "cantidad_presentacion": 1,
            "formato_presentacion": formato,
            "iva_porcentaje": 10,
            "merma": 0,
        })

    log_debug("PRODUCTOS TABLA", productos)
    return productos


# ======================================================
# LISTA VERTICAL (Cuevas)
# ======================================================
def parse_vertical(lines, precio_re, formato_re):
    productos = []
    i = 0

    while i < len(lines):
        line = lines[i]

        if linea_es_basura(line):
            i += 1
            continue

        # precios cerca
        pm = precio_re.search(line)
        precio = None
        skip = 0

        if pm:
            precio = float(pm.group(1).replace(",", "."))
        else:
            for j in range(1, 3):
                if i+j < len(lines):
                    pm2 = precio_re.search(lines[i+j])
                    if pm2:
                        precio = float(pm2.group(1).replace(",", "."))
                        skip = j
                        break

        if precio is None:
            i += 1
            continue

        # formato
        fm = formato_re.search(line)
        formato = fm.group(0) if fm else ""

        # nombre
        nombre = line
        if pm:
            nombre = nombre.replace(pm.group(1), "")
        if formato:
            nombre = nombre.replace(formato, "")

        nombre = nombre.strip(" -.").strip()
        nombre = normalizarNombre(nombre)

        # descartar nombres vacíos o muy cortos
        if not nombre or len(nombre) <= 2:
            i += skip + 1
            continue

        if linea_es_basura(nombre):
            i += skip + 1
            continue

        # unidad base según formato
        unidad_base = "unidad"
        if formato:
            um = re.search(r"(kg|g|gr|l|ml)", formato, re.IGNORECASE)
            if um:
                unidad_base = normalizarUnidad(um.group(1))

        productos.append({
            "nombre": nombre,
            "precio": precio,
            "unidad_base": unidad_base,
            "cantidad_presentacion": 1,
            "formato_presentacion": formato,
            "iva_porcentaje": 10,
            "merma": 0,
        })

        i += skip + 1

    log_debug("PRODUCTOS VERTICAL", productos)
    return productos


# ======================================================
# AUTOCORRECCIÓN SUAVE (sin eliminar letras)
# ======================================================
DICCIONARIO_PRODUCTOS = [
    "Jalapeños","Aguacate","Aguacate Hass","Pomelos",
    "Pimientos","Cebolla","Tomate","Manzana","Bananas",
    "Carne","Filete","Vacuno","Pierna","Melocotón","Granadas"
]

def autocorregir_nombre(nombre: str):
    n = nombre.strip().title()

    reemplazos = {
        "Jaapeños": "Jalapeños",
        "Auacate": "Aguacate",
        "Pomeos": "Pomelos",
    }

    for k,v in reemplazos.items():
        if k.lower() in n.lower():
            return v

    mejor = difflib.get_close_matches(n, DICCIONARIO_PRODUCTOS, n=1, cutoff=0.7)
    if mejor:
        return mejor[0]

    return n


# ======================================================
# NORMALIZAR NOMBRE DE PRODUCTO
# ======================================================
def normalizarNombre(nombre: str) -> str:
    if not nombre:
        return nombre

    # Quitar códigos numéricos al inicio: "000062 Rabanos Kg" → "Rabanos Kg"
    nombre = re.sub(r"^[0-9]{2,6}\s*", "", nombre)

    # Espacios múltiples
    nombre = re.sub(r"\s+", " ", nombre).strip(" .-").strip()

    if not nombre:
        return nombre

    # Autocorrección + title
    nombre = autocorregir_nombre(nombre)

    return nombre


# ======================================================
# PROCESAR UN JOB
# ======================================================
def process_job(job):
    print(f"\n======================")
    print(f"Procesando página {job['numero_pagina']}")
    print("======================\n")

    supabase.table("proveedor_listas_jobs").update(
        {"estado":"procesando"}
    ).eq("id", job["id"]).execute()

    try:
        text = ocr_google(job["archivo_url"])

        log_debug("RAW TEXT", text)

        items = parse_items_inteligente(text)
        print(f"✔ Detectados {len(items)} productos (antes de deduplicar)")

        # 🔁 Eliminar duplicados por nombre+precio+formato_presentacion
        unique = {}
        for it in items:
            key = f"{it['nombre']}|{it['precio']}|{it.get('formato_presentacion','')}"
            unique[key] = it

        items = list(unique.values())
        print(f"✔ Tras eliminar duplicados: {len(items)} productos")

        for item in items:
            item["proveedor_id"] = job["proveedor_id"]
            item["organizacion_id"] = job["organizacion_id"]
            item["creado_desde_archivo"] = job["lista_id"]
            item["pagina"] = job["numero_pagina"]

            supabase.table("proveedor_listas_items").upsert(
                item,
                on_conflict="proveedor_id,organizacion_id,nombre"
            ).execute()
            supabase.table("proveedor_listas_precios_historial").insert({
                "proveedor_id": job["proveedor_id"],
                "organizacion_id": job["organizacion_id"],
                "nombre": item["nombre"],
                "precio": item["precio"],
                "unidad_base": item["unidad_base"],
                "formato_presentacion": item["formato_presentacion"],
                "source_lista_id": job["lista_id"],
            }).execute()


        supabase.table("proveedor_listas_jobs").update(
            {"estado":"procesado"}
        ).eq("id", job["id"]).execute()

        print("OK")

    except Exception as e:
        print("ERROR:", e)
        supabase.table("proveedor_listas_jobs").update(
            {"estado":"error", "error":str(e)}
        ).eq("id", job["id"]).execute()


# ======================================================
# ACTUALIZAR PROGRESO
# ======================================================
def actualizar_progreso(lista_id):
    procesados = supabase.table("proveedor_listas_jobs").select("*",count="exact").eq(
        "lista_id", lista_id).eq("estado","procesado").execute().count

    total = supabase.table("proveedor_listas_jobs").select("*",count="exact").eq(
        "lista_id", lista_id).execute().count

    estado = "procesado" if procesados == total else "procesando"

    supabase.table("proveedor_listas").update({
        "lotes_procesados": procesados,
        "total_lotes": total,
        "estado": estado
    }).eq("id", lista_id).execute()

    print(f"📦 {procesados}/{total}")


# ======================================================
# MAIN
# ======================================================
def main():
    jobs = supabase.table("proveedor_listas_jobs") \
        .select("*") \
        .eq("estado", "pendiente") \
        .order("numero_pagina", desc=False) \
        .execute().data

    if not jobs:
        print("No pending jobs.")
        return

    print(f"🔍 {len(jobs)} jobs encontrados")

    for job in jobs:
        process_job(job)
        actualizar_progreso(job["lista_id"])

    print("✔ OCR COMPLETADO")


if __name__ == "__main__":
    main()
