import os
import logging
import time
from datetime import datetime, date
from decimal import Decimal
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
import requests

from etl_utils import upsert_rows  # usa a função existente (abre a própria conexão)

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
load_dotenv()

log = logging.getLogger("run_precadastros")
log.setLevel(logging.INFO)
_console = logging.StreamHandler()
_console.setLevel(logging.INFO)
_console.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s"))
log.addHandler(_console)

URL_PRECADASTROS = os.getenv(
    "URL_PRECADASTROS",
    "https://frjr.cvcrm.com.br/api/v1/cvdw/precadastros",
)
EMAIL = os.getenv("CVCRM_EMAIL") or ""
TOKEN = os.getenv("CVCRM_TOKEN") or ""

# Permite sobrescrever método por variável específica ou reaproveitar a de reservas
HTTP_METHOD = (os.getenv("PRECADASTROS_HTTP_METHOD")
               or os.getenv("RESERVAS_HTTP_METHOD")
               or "GET").upper()

PAGE_SIZE = int(os.getenv("RESERVAS_PAGE_SIZE") or os.getenv("CVCRM_PAGE_SIZE") or "450")
TIMEOUT = int(os.getenv("RESERVAS_TIMEOUT") or "50")
RETRIES = int(os.getenv("RESERVAS_RETRIES") or "3")
SINCE = os.getenv("CVCRM_SINCE")  # ex: "2023-01-01 00:00:00"

# Carregar arrays (tabela filha)
LOAD_PRE_CAMPOS_ADICIONAIS = (os.getenv("LOAD_PRE_CAMPOS_ADICIONAIS") or "1") == "1"

HEADERS = {
    "email": EMAIL,
    "token": TOKEN,
    "Accept": "application/json",
}
"""
# Proxy (opcional): respeita ENABLE_PROXY e limpa variáveis quando desabilitado
if (os.getenv("ENABLE_PROXY") or "0") == "1":
    os.environ["HTTP_PROXY"] = os.getenv("PROXY_HTTP", "")
    os.environ["HTTPS_PROXY"] = os.getenv("PROXY_HTTPS", "")
else:
    for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
        os.environ.pop(k, None)
"""

TABLE_BASE = "cv_precadastros"
TABLE_CA = "cv_precadastros_campos_adicionais"

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

def to_date_safe(s: Optional[str]) -> Optional[date]:
    dt = to_datetime_safe(s)
    return dt.date() if dt else None

def to_int_safe(v: Any) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None

def to_bigint_safe(v: Any) -> Optional[int]:
    return to_int_safe(v)

def to_decimal_safe(v: Any) -> Optional[Decimal]:
    if v is None or v == "":
        return None
    try:
        return Decimal(str(v))
    except Exception:
        return None

# -----------------------------------------------------------------------------
# API
# -----------------------------------------------------------------------------
def fetch_page(page: int, since: Optional[str] = None) -> Dict[str, Any]:
    """
    Busca uma página de pré-cadastros com retries e backoff simples.
    """
    body = {
        "pagina": page,
        "registros_por_pagina": PAGE_SIZE,
    }
    if since:
        # Ajuste o nome do parâmetro conforme a API exigir:
        body["a_partir_data_cad"] = since

    for attempt in range(1, RETRIES + 1):
        try:
            if HTTP_METHOD == "POST":
                headers = {**HEADERS, "Content-Type": "application/json"}
                resp = requests.post(URL_PRECADASTROS, json=body, headers=headers, timeout=TIMEOUT)
            else:
                resp = requests.get(URL_PRECADASTROS, params=body, headers=HEADERS, timeout=TIMEOUT)

            resp.raise_for_status()
            return resp.json() or {}
        except requests.RequestException as e:
            log.warning(f"Falha ao chamar API (tentativa {attempt}/{RETRIES}) página={page}: {e}")
            if attempt == RETRIES:
                break
            time.sleep(2 * attempt)
    return {}

# -----------------------------------------------------------------------------
# Normalização — base (cv_precadastros)
# -----------------------------------------------------------------------------
def normalize_rows(dados: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for it in dados or []:
        idprecadastro = it.get("idprecadastro")
        if not idprecadastro:
            log.error("Registro sem idprecadastro: %s", it)
            continue

        row: Dict[str, Any] = {
            # PK
            "idprecadastro": idprecadastro,

            # Identificação / status
            "referencia": it.get("referencia"),
            "referencia_data": to_datetime_safe(it.get("referencia_data")),
            "ativo": (it.get("ativo") or None),
            "codigointerno": it.get("codigointerno"),
            "idsituacao": to_int_safe(it.get("idsituacao")),
            "situacao": it.get("situacao"),
            "condicao_aprovada": (it.get("condicao_aprovada") or None),

            # Relacionamentos
            "idempreendimento": to_bigint_safe(it.get("idempreendimento")),
            "empreendimento": it.get("empreendimento"),
            "idunidade": to_bigint_safe(it.get("idunidade")),
            "unidade": it.get("unidade"),
            "idcorretor": to_bigint_safe(it.get("idcorretor")),
            "corretor": it.get("corretor"),
            "idimobiliaria": to_bigint_safe(it.get("idimobiliaria")),
            "imobiliaria": it.get("imobiliaria"),
            "idempresa": to_bigint_safe(it.get("idempresa")),
            "empresa": it.get("empresa"),
            "idpessoa": to_bigint_safe(it.get("idpessoa")),

            # Cliente e contato
            "pessoa": it.get("pessoa"),
            "cep_cliente": it.get("cep_cliente"),

            # Correspondente
            "idusuario_correspondente": to_bigint_safe(it.get("idusuario_correspondente")),
            "usuario_correspondente": it.get("usuario_correspondente"),
            "empresa_correspondente": it.get("empresa_correspondente"),

            # Leads (pode vir "62682,65286")
            "idlead": it.get("idlead"),

            # Financeiro
            "renda_cliente_principal": to_decimal_safe(it.get("renda_cliente_principal")),
            "valor_avaliacao": to_decimal_safe(it.get("valor_avaliacao")),
            "valor_aprovado": to_decimal_safe(it.get("valor_aprovado")),
            "valor_subsidio": to_decimal_safe(it.get("valor_subsidio")),
            "valor_total": to_decimal_safe(it.get("valor_total")),
            "valor_fgts": to_decimal_safe(it.get("valor_fgts")),
            "saldo_devedor": to_decimal_safe(it.get("saldo_devedor")),
            "valor_prestacao": to_decimal_safe(it.get("valor_prestacao")),
            "renda_total": to_decimal_safe(it.get("renda_total")),

            # Condições/planos
            "prazo": to_int_safe(it.get("prazo")),
            "observacoes": it.get("observacoes"),
            "tabela": it.get("tabela"),
            "carta_credito": it.get("carta_credito"),
            "vencimento_aprovacao": to_date_safe(it.get("vencimento_aprovacao")),

            # Motivos / cancelamentos
            "idmotivo_reprovacao": to_int_safe(it.get("idmotivo_reprovacao")),
            "motivo_reprovacao": it.get("motivo_reprovacao"),
            "descricao_motivo_reprovacao": it.get("descricao_motivo_reprovacao"),
            "idmotivo_cancelamento": to_int_safe(it.get("idmotivo_cancelamento")),
            "motivo_cancelamento": it.get("motivo_cancelamento"),
            "descricao_motivo_cancelamento": it.get("descricao_motivo_cancelamento"),

            # SLA / datas de controle
            "sla_vencimento": to_int_safe(it.get("sla_vencimento")),
            "data_cad": to_datetime_safe(it.get("data_cad")),
            "idsituacao_anterior": to_int_safe(it.get("idsituacao_anterior")),
            "situacao_anterior": it.get("situacao_anterior"),
            "data_ultima_alteracao_situacao": to_datetime_safe(it.get("data_ultima_alteracao_situacao")),

            # Intenção de compra
            "idintencao_compra": to_int_safe(it.get("idintencao_compra")),
            "intencao_compra": it.get("intencao_compra"),
        }

        rows.append(row)
    return rows

# -----------------------------------------------------------------------------
# Normalização — tabela filha (campos_adicionais)
# -----------------------------------------------------------------------------
def normalize_campos_adicionais(dados: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for it in dados or []:
        idprecadastro = it.get("idprecadastro")
        lista = it.get("campos_adicionais") or []
        for ca in lista:
            row = {
                "idprecadastro": idprecadastro,
                "referencia": ca.get("referencia"),  # VARCHAR(32)
                "referencia_data": to_datetime_safe(ca.get("referencia_data")),
                "idcampo_valores": to_bigint_safe(ca.get("idcampo_valores")),
                "idcampo": to_bigint_safe(ca.get("idcampo")),
                "nome": ca.get("nome"),
                "valor": ca.get("valor"),
                "tipo": ca.get("tipo"),
            }
            rows.append(row)
    return rows

# -----------------------------------------------------------------------------
# Runner
# -----------------------------------------------------------------------------
def run(api_name: str = "cv_precadastros") -> None:
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
            total_upserts += upsert_rows(None, TABLE_BASE, base_rows, pk_columns=["idprecadastro"])

        # Filha
        if LOAD_PRE_CAMPOS_ADICIONAIS:
            ca_rows = normalize_campos_adicionais(dados_page)
            if ca_rows:
                total_upserts += upsert_rows(None, TABLE_CA, ca_rows, pk_columns=["idprecadastro", "idcampo_valores"])

        log.info(f"[{api_name}] Página {page} concluída. Registros API: {len(dados_page)} | Upserts acumulados: {total_upserts}")
        page += 1

    log.info(f"[{api_name}] Finalizado. Registros recebidos da API: {total_registros_api} | Upserts totais: {total_upserts}")

if __name__ == "__main__":
    run("cv_precadastros")