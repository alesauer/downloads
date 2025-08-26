import os
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
import requests

from etl_utils import upsert_rows  # usa a função existente (abre a própria conexão)

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
load_dotenv()

log = logging.getLogger("run_visitas")
log.setLevel(logging.INFO)
_console = logging.StreamHandler()
_console.setLevel(logging.INFO)
_console.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s"))
log.addHandler(_console)

URL_VISITAS = os.getenv("URL_VISITAS", "https://frjr.cvcrm.com.br/api/v1/cvdw/visitas")
EMAIL = os.getenv("CVCRM_EMAIL") or ""
TOKEN = os.getenv("CVCRM_TOKEN") or ""

HTTP_METHOD = (os.getenv("VISITAS_HTTP_METHOD")
               or os.getenv("RESERVAS_HTTP_METHOD")
               or "GET").upper()

PAGE_SIZE = int(os.getenv("RESERVAS_PAGE_SIZE") or os.getenv("CVCRM_PAGE_SIZE") or "450")
TIMEOUT = int(os.getenv("RESERVAS_TIMEOUT") or "30")
RETRIES = int(os.getenv("RESERVAS_RETRIES") or "3")
SINCE = os.getenv("CVCRM_SINCE")  # ex: "2023-01-01 00:00:00"

HEADERS = {
    "email": EMAIL,
    "token": TOKEN,
    "Accept": "application/json",
}

# Proxy (opcional): respeita ENABLE_PROXY e limpa variáveis quando desabilitado
if (os.getenv("ENABLE_PROXY") or "0") == "1":
    os.environ["HTTP_PROXY"]  = os.getenv("PROXY_HTTP", "")
    os.environ["HTTPS_PROXY"] = os.getenv("PROXY_HTTPS", "")
else:
    for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
        os.environ.pop(k, None)

TABLE_NAME = "cv_visitas"

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
DATE_PATTERNS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d")

def to_datetime_safe(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    for fmt in DATE_PATTERNS:
        try:
            return datetime.strptime(s, fmt)
        except (ValueError, TypeError):
            continue
    return None

def to_int_safe(v: Any) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None

def to_bigint_safe(v: Any) -> Optional[int]:
    return to_int_safe(v)

def to_char_sn(v: Any) -> Optional[str]:
    """Normaliza flags 'S'/'N' (ou None)."""
    if v is None or v == "":
        return None
    v = str(v).strip().upper()
    return v if v in ("S", "N") else v[:1]  # mantém 'S'/'N' se vier correto

# -----------------------------------------------------------------------------
# API
# -----------------------------------------------------------------------------
def fetch_page(page: int, since: Optional[str] = None) -> Dict[str, Any]:
    """
    Busca uma página de visitas com retries e backoff simples.
    """
    body = {
        "pagina": page,
        "registros_por_pagina": PAGE_SIZE,
    }
    if since:
        # Ajuste o nome do parâmetro conforme a API exigir (mantendo padrão dos outros ETLs):
        body["a_partir_data_cad"] = since

    for attempt in range(1, RETRIES + 1):
        try:
            if HTTP_METHOD == "POST":
                headers = {**HEADERS, "Content-Type": "application/json"}
                resp = requests.post(URL_VISITAS, json=body, headers=headers, timeout=TIMEOUT)
            else:
                resp = requests.get(URL_VISITAS, params=body, headers=HEADERS, timeout=TIMEOUT)

            resp.raise_for_status()
            return resp.json() or {}
        except requests.RequestException as e:
            log.warning(f"Falha ao chamar API (tentativa {attempt}/{RETRIES}) página={page}: {e}")
            if attempt == RETRIES:
                break
            time.sleep(2 * attempt)
    return {}

# -----------------------------------------------------------------------------
# Normalização — base (cv_visitas)
# -----------------------------------------------------------------------------
def normalize_rows(dados: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for it in dados or []:
        idtarefa = it.get("idtarefa")
        if not idtarefa:
            log.error("Registro sem idtarefa: %s", it)
            continue

        row: Dict[str, Any] = {
            # PK
            "idtarefa": idtarefa,

            # Metadados/identificação
            "referencia": it.get("referencia"),
            "referencia_data": to_datetime_safe(it.get("referencia_data")),
            "ativo": to_char_sn(it.get("ativo")),

            # Datas e status
            "data_cad": to_datetime_safe(it.get("data_cad")),
            "data": to_datetime_safe(it.get("data")),
            "situacao": it.get("situacao"),

            # Responsável principal
            "idresponsavel": to_bigint_safe(it.get("idresponsavel")),
            "tipo_responsavel": (str(it.get("tipo_responsavel")).strip().upper() if it.get("tipo_responsavel") else None),
            "responsavel": it.get("responsavel"),

            # Lead e interação
            "funcionalidade": it.get("funcionalidade"),
            "idlead": str(it.get("idlead")) if it.get("idlead") is not None else None,
            "idinteracao": to_bigint_safe(it.get("idinteracao")),
            "tipo_interacao": it.get("tipo_interacao"),
            "data_conclusao": to_datetime_safe(it.get("data_conclusao")),

            # Tipo de visita e flags
            "idtipo_visita": to_int_safe(it.get("idtipo_visita")),
            "nome_tipo_visita": it.get("nome_tipo_visita"),
            "visita_virtual": to_char_sn(it.get("visita_virtual")),

            # PDV
            "pdv": it.get("pdv"),
            "painel_pdv": to_char_sn(it.get("painel_pdv")),

            # Quem criou
            "idresponsavel_por_criar_visita": to_bigint_safe(it.get("idresponsavel_por_criar_visita")),
            "responsavel_por_criar_visita": it.get("responsavel_por_criar_visita"),

            # Empreendimento
            "idempreendimento": to_bigint_safe(it.get("idempreendimento")),
            "nome_empreendimento": it.get("nome_empreendimento"),
        }

        rows.append(row)
    return rows

# -----------------------------------------------------------------------------
# Runner
# -----------------------------------------------------------------------------
def run(api_name: str = "cv_visitas") -> None:
    page = 1
    total_pages = 1
    since = SINCE

    total_upserts = 0
    total_registros_api = 0

    while page <= total_pages:
        log.info(f"[{api_name}] Processando página {page}/{total_pages} ...")

        data = fetch_page(page, since)
        if not data:
            log.error(f"[{api_name}] Página {page}: resposta vazia/erro. Encerrando.")
            break

        dados_page: List[Dict[str, Any]] = data.get("dados") or []
        total_pages = int(data.get("total_de_paginas") or data.get("total_pages") or total_pages)
        total_registros_api += len(dados_page)

        # Base
        base_rows = normalize_rows(dados_page)
        if base_rows:
            total_upserts += upsert_rows(None, TABLE_NAME, base_rows, pk_columns=["idtarefa"])

        log.info(f"[{api_name}] Página {page} concluída. Registros API: {len(dados_page)} | Upserts acumulados: {total_upserts}")
        page += 1

    log.info(f"[{api_name}] Finalizado. Registros recebidos da API: {total_registros_api} | Upserts totais: {total_upserts}")

if __name__ == "__main__":
    run("cv_visitas")
