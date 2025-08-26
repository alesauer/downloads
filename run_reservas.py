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

log = logging.getLogger("run_reservas")
log.setLevel(logging.INFO)
_console = logging.StreamHandler()
_console.setLevel(logging.INFO)
_console.setFormatter(logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s"))
log.addHandler(_console)

URL_RESERVAS = os.getenv("URL_RESERVAS", "https://frjr.cvcrm.com.br/api/v1/cvdw/reservas")
EMAIL = os.getenv("CVCRM_EMAIL") or ""
TOKEN = os.getenv("CVCRM_TOKEN") or ""
HTTP_METHOD = (os.getenv("RESERVAS_HTTP_METHOD") or "GET").upper()  # GET ou POST
PAGE_SIZE = int(os.getenv("RESERVAS_PAGE_SIZE") or os.getenv("CVCRM_PAGE_SIZE") or "450")
TIMEOUT = int(os.getenv("RESERVAS_TIMEOUT") or "30")
RETRIES = int(os.getenv("RESERVAS_RETRIES") or "3")
SINCE = os.getenv("CVCRM_SINCE")  # ex: "2023-01-01 00:00:00"

# Carregar arrays (se tabelas filhas foram criadas)
LOAD_CAMPOS_ADICIONAIS = (os.getenv("LOAD_CAMPOS_ADICIONAIS") or "1") == "1"
LOAD_CAMPOS_ADICIONAIS_CONTRATO = (os.getenv("LOAD_CAMPOS_ADICIONAIS_CONTRATO") or "1") == "1"

HEADERS = {
    "email": EMAIL,
    "token": TOKEN,
    "Accept": "application/json",
}


# Proxy (opcional): respeita ENABLE_PROXY e limpa variáveis quando desabilitado
if (os.getenv("ENABLE_PROXY") or "0") == "1":
    os.environ["HTTP_PROXY"] = os.getenv("PROXY_HTTP", "")
    os.environ["HTTPS_PROXY"] = os.getenv("PROXY_HTTPS", "")
else:
    for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
        os.environ.pop(k, None)



TABLE_NAME = "cv_reservas"
TABLE_CA = "cv_reservas_campos_adicionais"
TABLE_CAC = "cv_reservas_campos_adicionais_contrato"

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
    Busca uma página de reservas com retries e backoff simples.
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
                resp = requests.post(URL_RESERVAS, json=body, headers=headers, timeout=TIMEOUT)
            else:
                resp = requests.get(URL_RESERVAS, params=body, headers=HEADERS, timeout=TIMEOUT)

            resp.raise_for_status()
            return resp.json() or {}
        except requests.RequestException as e:
            log.warning(f"Falha ao chamar API (tentativa {attempt}/{RETRIES}) página={page}: {e}")
            if attempt == RETRIES:
                break
            time.sleep(2 * attempt)
    return {}


# -----------------------------------------------------------------------------
# Normalização — base (cv_reservas)
# -----------------------------------------------------------------------------
def normalize_rows(dados: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for it in dados or []:
        idreserva = it.get("idreserva")
        if not idreserva:
            log.error("Registro sem idreserva: %s", it)
            continue

        row: Dict[str, Any] = {
            # PK
            "idreserva": idreserva,

            # Campos do schema
            "referencia": it.get("referencia"),
            "referencia_data": to_datetime_safe(it.get("referencia_data")),
            "ativo": (it.get("ativo") or None),

            "codigointerno": it.get("codigointerno"),
            "numero_venda": it.get("numero_venda"),
            "aprovada": it.get("aprovada"),

            "data_cad": to_datetime_safe(it.get("data_cad")),
            "data_venda": to_datetime_safe(it.get("data_venda")),
            "situacao": it.get("situacao"),
            "idsituacao": to_int_safe(it.get("idsituacao")),
            "situacao_comercial": it.get("situacao_comercial"),

            "idempreendimento": to_bigint_safe(it.get("idempreendimento")),
            "codigointerno_empreendimento": it.get("codigointerno_empreendimento"),
            "empreendimento": it.get("empreendimento"),

            "data_entrega_chaves_contrato_cliente": to_date_safe(it.get("data_entrega_chaves_contrato_cliente")),
            "etapa": it.get("etapa"),
            "bloco": it.get("bloco"),
            "unidade": it.get("unidade"),
            "regiao": it.get("regiao"),
            "venda": it.get("venda"),

            "idcliente": to_bigint_safe(it.get("idcliente")),
            "documento_cliente": it.get("documento_cliente"),
            "cliente": it.get("cliente"),
            "email": it.get("email"),
            "cidade": it.get("cidade"),
            "cep_cliente": it.get("cep_cliente"),

            "renda": to_decimal_safe(it.get("renda")),
            "sexo": it.get("sexo"),
            "idade": to_int_safe(it.get("idade")),
            "estado_civil": it.get("estado_civil"),

            "idcorretor": to_bigint_safe(it.get("idcorretor")),
            "corretor": it.get("corretor"),

            "idimobiliaria": to_bigint_safe(it.get("idimobiliaria")),
            "imobiliaria": it.get("imobiliaria"),

            "idtime": to_int_safe(it.get("idtime")),
            "nome_time": it.get("nome_time"),

            "valor_contrato": to_decimal_safe(it.get("valor_contrato")),
            "vencimento": to_datetime_safe(it.get("vencimento")),
            "campanha": it.get("campanha"),
            "cessao": it.get("cessao"),
            "motivo_cancelamento": it.get("motivo_cancelamento"),
            "data_cancelamento": to_datetime_safe(it.get("data_cancelamento")),
            "espacos_complementares": it.get("espacos_complementares"),

            "idlead": it.get("idlead"),
            "data_ultima_alteracao_situacao": to_datetime_safe(it.get("data_ultima_alteracao_situacao")),

            "idempresa_correspondente": to_bigint_safe(it.get("idempresa_correspondente")),
            "empresa_correspondente": it.get("empresa_correspondente"),

            "valor_fgts": to_decimal_safe(it.get("valor_fgts")),
            "valor_financiamento": to_decimal_safe(it.get("valor_financiamento")),
            "valor_subsidio": to_decimal_safe(it.get("valor_subsidio")),

            "nome_usuario": it.get("nome_usuario"),
            "idunidade": to_bigint_safe(it.get("idunidade")),
            "idprecadastro": to_bigint_safe(it.get("idprecadastro")),
            "idmidia": to_bigint_safe(it.get("idmidia")),
            "midia": it.get("midia"),
            "descricao_motivo_cancelamento": it.get("descricao_motivo_cancelamento"),

            "idsituacao_anterior": to_int_safe(it.get("idsituacao_anterior")),
            "situacao_anterior": it.get("situacao_anterior"),

            "idtabela": to_bigint_safe(it.get("idtabela")),
            "nometabela": it.get("nometabela"),
            "codigointernotabela": it.get("codigointernotabela"),
            "idtipo_tabela": to_int_safe(it.get("idtipo_tabela")),
            "tipo_tabela": it.get("tipo_tabela"),

            "data_contrato": to_date_safe(it.get("data_contrato")),
            "valor_proposta": to_decimal_safe(it.get("valor_proposta")),
            "vpl_reserva": to_decimal_safe(it.get("vpl_reserva")),
            "vgv_tabela": to_decimal_safe(it.get("vgv_tabela")),
            "vpl_tabela": to_decimal_safe(it.get("vpl_tabela")),

            "usuario_aprovacao": it.get("usuario_aprovacao"),
            "data_aprovacao": to_date_safe(it.get("data_aprovacao")),
            "juros_condicao_aprovada": to_decimal_safe(it.get("juros_condicao_aprovada")),
            "juros_apos_entrega_condicao_aprovada": to_decimal_safe(it.get("juros_apos_entrega_condicao_aprovada")),
            "idtabela_condicao_aprovada": to_bigint_safe(it.get("idtabela_condicao_aprovada")),
            "data_primeira_aprovacao": to_date_safe(it.get("data_primeira_aprovacao")),
            "aprovacao_absoluto": to_decimal_safe(it.get("aprovacao_absoluto")),
            "aprovacao_vpl_valor": to_decimal_safe(it.get("aprovacao_vpl_valor")),

            "idtipovenda": to_int_safe(it.get("idtipovenda")),
            "tipovenda": it.get("tipovenda"),
            "idgrupo": to_int_safe(it.get("idgrupo")),
            "grupo": it.get("grupo"),

            "data_modificacao": to_datetime_safe(it.get("data_modificacao")),
        }

        rows.append(row)
    return rows


# -----------------------------------------------------------------------------
# Normalização — tabelas filhas (arrays)
# -----------------------------------------------------------------------------
def normalize_campos_adicionais(dados: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for it in dados or []:
        idreserva = it.get("idreserva")
        lista = it.get("campos_adicionais") or []
        for ca in lista:
            row = {
                "idreserva": idreserva,
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


def normalize_campos_adicionais_contrato(dados: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for it in dados or []:
        idreserva = it.get("idreserva")
        lista = it.get("campos_adicionais_contrato") or []
        for ca in lista:
            row = {
                "idreservacontratocampoadicional": to_bigint_safe(ca.get("idreservacontratocampoadicional")),
                "idreserva": idreserva,
                "referencia": ca.get("referencia"),  # VARCHAR(32)
                "referencia_data": to_datetime_safe(ca.get("referencia_data")),
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
def run(api_name: str = "cv_reservas") -> None:
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
            total_upserts += upsert_rows(None, TABLE_NAME, base_rows, pk_columns=["idreserva"])

        # Filhas
        if LOAD_CAMPOS_ADICIONAIS:
            ca_rows = normalize_campos_adicionais(dados_page)
            if ca_rows:
                total_upserts += upsert_rows(None, TABLE_CA, ca_rows, pk_columns=["idreserva", "idcampo_valores"])

        if LOAD_CAMPOS_ADICIONAIS_CONTRATO:
            cac_rows = normalize_campos_adicionais_contrato(dados_page)
            if cac_rows:
                total_upserts += upsert_rows(None, TABLE_CAC, cac_rows, pk_columns=["idreservacontratocampoadicional"])

        log.info(f"[{api_name}] Página {page} concluída. Registros API: {len(dados_page)} | Upserts acumulados: {total_upserts}")
        page += 1

    log.info(f"[{api_name}] Finalizado. Registros recebidos da API: {total_registros_api} | Upserts totais: {total_upserts}")


if __name__ == "__main__":
    run("cv_reservas")