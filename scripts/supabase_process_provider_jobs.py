import os
import tempfile
import requests
from typing import List, Dict, Any

from supabase import create_client, Client
from paddleocr import PaddleOCR


SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]


def get_supabase_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


# -----------------------------------------------------------
# Parser similar a tu parseProveedor de Deno
# -----------------------------------------------------------
def parse_proveedor(texto: str, proveedor_id: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for raw_line in texto.split("\n"):
        line = raw_line.strip()
        if len(line) < 3:
            continue

        # último número con 1 o 2 decimales al final
        import re
        m = re.search(r"(\d+[.,]\d{1,2})\s*$", line)
        if not m:
            continue

        precio_str = m.group(1)
        try:
            precio = float(precio_str.replace(",", "."))
        except ValueError:
            continue

        nombre = line.replace(precio_str, "").strip()
        nombre = " ".join(nombre.split())
        if len(nombre) < 2:
            continue

        items.append(
            {
                "nombre": nombre,
                "precio": precio,
                "unidad": "unidad",
                "cantidad": 1,
            }
        )

    print(f"🟦 ITEMS PARSEADOS ({len(items)}): {items}")
    return items


# -----------------------------------------------------------
# OCR con PaddleOCR
# -----------------------------------------------------------
def ocr_page_image(path: str) -> str:
    # lang="es" o "latin" según veas mejor luego
    ocr = PaddleOCR(use_angle_cls=True, lang="es", show_log=False)
    result = ocr.ocr(path, cls=True)

    lines: List[str] = []
    for page in result:
        for line in page:
            text = line[1][0]
            lines.append(text)

    texto = "\n".join(lines)
    print("📄 TEXTO OCR (recortado):")
    print("\n".join(lines[:10]))
    return texto


# -----------------------------------------------------------
# Procesar jobs pendientes
# -----------------------------------------------------------
def process_jobs():
    supabase = get_supabase_client()

    # 1) Buscar jobs pendientes (puedes limitar para ir despacio)
    resp = (
        supabase.table("proveedor_listas_jobs")
        .select("*")
        .eq("estado", "pendiente")
        .order("numero_pagina", desc=False)
        .limit(5)
        .execute()
    )
    jobs = resp.data or []
    if not jobs:
        print("✅ No pending jobs")
        return

    print(f"⚙️ Encontrados {len(jobs)} jobs pendientes")

    for job in jobs:
        job_id = job["id"]
        lista_id = job["lista_id"]
        proveedor_id = job["proveedor_id"]
        org_id = job["organizacion_id"]
        archivo_url = job["archivo_url"]
        numero_pagina = job.get("numero_pagina")

        print(f"📄 Procesando job {job_id} (lista {lista_id}) página {numero_pagina}")

        # 2) Marcar como procesando
        supabase.table("proveedor_listas_jobs").update(
            {"estado": "procesando"}
        ).eq("id", job_id).execute()

        try:
            # 3) Descargar la imagen
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as tmp:
                print(f"⬇️ Descargando imagen: {archivo_url}")
                r = requests.get(archivo_url, timeout=60)
                r.raise_for_status()
                tmp.write(r.content)
                tmp_path = tmp.name

            # 4) OCR
            texto = ocr_page_image(tmp_path)
            if not texto.strip():
                raise RuntimeError("OCR vacío en esta página")

            # 5) Parsear
            items = parse_proveedor(texto, proveedor_id)
            if not items:
                print("⚠️ No se encontraron items parseados en esta página")

            # 6) Insertar items en proveedor_listas_items
            for item in items:
                data_row = {
                    "proveedor_id": proveedor_id,
                    "organizacion_id": org_id,
                    "nombre": item["nombre"],
                    "precio": float(item["precio"]),
                    "unidad_base": item.get("unidad", "unidad"),
                    "cantidad_presentacion": float(item.get("cantidad", 1)),
                    "formato_presentacion": item.get("unidad", ""),
                    "iva_porcentaje": 10,
                    "merma": 0,
                    "creado_desde_archivo": lista_id,
                    "pagina": numero_pagina,  # columna que añadiremos
                }
                ins = (
                    supabase.table("proveedor_listas_items")
                    .insert(data_row)
                    .execute()
                )
                if ins.data:
                    print(f"✅ Item insertado: {item['nombre']}")

            # 7) Marcar job como procesado
            supabase.table("proveedor_listas_jobs").update(
                {"estado": "procesado", "error": None}
            ).eq("id", job_id).execute()

        except Exception as e:
            print(f"❌ Error procesando job {job_id}: {e}")
            supabase.table("proveedor_listas_jobs").update(
                {"estado": "error", "error": str(e)}
            ).eq("id", job_id).execute()
            continue

        # 8) Actualizar progreso en proveedor_listas
        proc_resp = (
            supabase.table("proveedor_listas_jobs")
            .select("id", count="exact")
            .eq("lista_id", lista_id)
            .eq("estado", "procesado")
            .execute()
        )
        total_resp = (
            supabase.table("proveedor_listas_jobs")
            .select("id", count="exact")
            .eq("lista_id", lista_id)
            .execute()
        )

        procesados = proc_resp.count or 0
        total = total_resp.count or 0
        estado_lista = "procesado" if procesados == total and total > 0 else "procesando"

        supabase.table("proveedor_listas").update(
            {
                "lotes_procesados": procesados,
                "total_lotes": total,
                "estado": estado_lista,
            }
        ).eq("id", lista_id).execute()

        print(f"📦 Progreso lista {lista_id}: {procesados}/{total} — {estado_lista}")


if __name__ == "__main__":
    process_jobs()
