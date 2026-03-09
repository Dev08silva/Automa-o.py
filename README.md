# Automa-o.py
Automação de atividades

#meu código, foi criado para unificar arquivos em txt para o excel e no mesmo código a criaão de um arquivo txt unificado com os valores de cada aplicação.

from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import re
import logging
import pdfplumber

# ================= CONFIGURAÇÕES =================
DESKTOP = Path.home() / "Desktop"
BASE_DIR = DESKTOP / "extratos"

PASTA_ENTRADA = BASE_DIR / "entrada"
PASTA_SAIDA   = BASE_DIR / "saida"
PASTA_LOGS    = BASE_DIR / "logs"

PASTA_SAIDA.mkdir(parents=True, exist_ok=True)
PASTA_LOGS.mkdir(parents=True, exist_ok=True)

LOG_FILE = PASTA_LOGS / "processamento.log"

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    encoding="utf-8"
)

VERSAO_SCRIPT = "v1.5 – RC Patch + Auditoria (CDB x TXT x Carteira92) 2026-02-24"

# ================= PARÂMETROS =================
OFFSET_DIAS_CABECALHO = 1  # D-1
GERAR_AUDITORIA = True
TRAVAR_SE_DIVERGIR = False  # se True, interrompe execução caso auditoria encontre diferença

# ================= REGEX =================
MONEY_RX = re.compile(r"\d{1,3}(?:\.\d{3})*,\d{2}")
MONEY_BIG_RX = re.compile(r"\d{1,3}(?:\.\d{3})+,\d{2}")  # exige separador de milhar
TAXA_RX  = re.compile(r"\d{1,3},\d{2}%CDI")
DATE_RX  = re.compile(r"\d{2}/\d{2}/\d{4}")

# ================= HELPERS =================
def fmt_brl(x) -> str:
    if pd.isna(x):
        return "0,00"
    s = str(x).strip()
    try:
        y = float(s.replace(".", "").replace(",", "."))
        return f"{y:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return s

def valor_float(x) -> float:
    if pd.isna(x):
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if not s:
        return 0.0
    try:
        return float(s.replace(".", "").replace(",", "."))
    except Exception:
        return 0.0

def venc_iso(v) -> str:
    if pd.isna(v):
        return ""
    if isinstance(v, str):
        for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(v, fmt).strftime("%Y-%m-%d")
            except Exception:
                pass
        return v
    try:
        return pd.to_datetime(v).strftime("%Y-%m-%d")
    except Exception:
        return str(v)

def inferir_tipo_produto(row: pd.Series):
    texto = " ".join(str(v) for v in row.values).upper()
    if "SELIC" in texto or "LFT" in texto:
        return "SELIC", "LFT_BRBCARD"
    return "cdi", "CDB_CDI_BRBCARD"

def taxa_fixa_por_produto(row: pd.Series) -> str:
    _, produto = inferir_tipo_produto(row)
    return "100,00" if produto == "LFT_BRBCARD" else "102,00"

def norm(v: str) -> str:
    v = "" if v is None else str(v).strip()
    return v if v else "0,00"

def ratio_small(a: str, ref: str, lim=0.05) -> bool:
    af = valor_float(a)
    rf = valor_float(ref)
    if rf <= 0:
        return False
    return (af / rf) < lim


# =============== PARSER DE RC (SEU PATCH) =================
def extrair_recompra_rc(rec_lines):
    """Pega a primeira data após o token RC (se existir)."""
    for ln in rec_lines:
        if "RC" in ln.split():
            toks = ln.split()
            try:
                idx = toks.index("RC")
            except ValueError:
                continue
            for t in toks[idx:]:
                if DATE_RX.fullmatch(t):
                    return t
    return None

def extrair_campos_rc(rec_lines):
    """
    Extrai a partir do bloco que contém 'RC' os campos:
      (rend_per, rend_acum, ir, iof, resg_bruto, resg_liq)
    """
    rc_start = None
    for i, ln in enumerate(rec_lines):
        if "RC" in ln.split():
            rc_start = i
            break
    if rc_start is None:
        return None

    rc_line = rec_lines[rc_start]
    after_rc_lines = rec_lines[rc_start + 1 :]

    vals_rc = MONEY_RX.findall(rc_line)
    smalls = [v for v in vals_rc if valor_float(v) < 1_000_000]
    bigs   = [v for v in vals_rc if valor_float(v) >= 1_000_000]

    resg_bruto = norm(bigs[0]) if bigs else "0,00"

    rend_per = "0,00"
    rend_acum = "0,00"
    ir = "0,00"

    if len(smalls) >= 3:
        sm_sorted = sorted(smalls, key=valor_float)
        ir        = norm(sm_sorted[0])
        rend_per  = norm(sm_sorted[1])
        rend_acum = norm(sm_sorted[-1])
    elif len(smalls) == 2:
        sm_sorted = sorted(smalls, key=valor_float)
        rend_per, rend_acum = norm(sm_sorted[0]), norm(sm_sorted[1])
    elif len(smalls) == 1:
        v = valor_float(smalls[0])
        if v <= 2_000:
            ir = norm(smalls[0])
        else:
            rend_per = norm(smalls[0])

    iof = "0,00"
    resg_liq = "0,00"
    for ln in after_rc_lines:
        if "%CDI" in ln:
            continue
        nums = MONEY_RX.findall(ln)
        if len(nums) == 2:
            a, b = nums[0], nums[1]
            if valor_float(a) <= valor_float(b):
                iof, resg_liq = norm(a), norm(b)
            else:
                iof, resg_liq = norm(b), norm(a)
            break
        elif len(nums) == 1:
            if valor_float(nums[0]) >= 1_000_000:
                resg_liq = norm(nums[0])

    if valor_float(resg_liq) == 0 and valor_float(resg_bruto) > 0:
        resg_liq_calc = valor_float(resg_bruto) - valor_float(ir) - valor_float(iof)
        if resg_liq_calc > 0:
            resg_liq = fmt_brl(resg_liq_calc)

    return (rend_per, rend_acum, ir, iof, resg_bruto, resg_liq)


# ================= GERAÇÃO DO TXT =================
def gerar_txt(df_final: pd.DataFrame, nome_txt: str, data_header: str, data_extra: str) -> Path:
    linhas = [f"@{data_header};BRBCARDTITULOS"]
    contador_validas = 0

    for _, row in df_final.iterrows():
        vlr = valor_float(row.get("SALDO_EM_D2"))
        if vlr == 0:
            continue

        valor_fmt = fmt_brl(row.get("SALDO_EM_D2"))
        taxa = taxa_fixa_por_produto(row)
        tipo, produto = inferir_tipo_produto(row)
        venc = venc_iso(row.get("VENCIMENTO"))

        linhas.append(f"{tipo};{produto};0;{valor_fmt};{taxa};{venc};")
        contador_validas += 1

    # Mantido como você tinha (extras com zero). Amanhã podemos evoluir para puxar do PDF/Carteira.
    linhas_extras = [
        "SELIC;LFT_BRBCARD;0;0,00;100,00;2028-03-01;",
        "SELIC;LFT_BRBCARD;0;0,00;100,00;2029-09-01;",
        f"SRM;CAIXA_BRBCARD;0;0,00;{data_extra};1;"
    ]
    linhas.extend(linhas_extras)

    total_linhas = contador_validas + len(linhas_extras)
    linhas.append(f"@{total_linhas}")

    caminho_txt = PASTA_SAIDA / nome_txt
    caminho_txt.write_text("\n".join(linhas) + "\n", encoding="latin-1")
    logging.info(
        f"TXT consolidado gerado: {caminho_txt} "
        f"(validas_planilha={contador_validas} + extras={len(linhas_extras)} => total={total_linhas})"
    )
    return caminho_txt


# ================= PARSER PRINCIPAL =================
def processar_arquivo(caminho_txt: Path) -> pd.DataFrame:
    logging.info(f"Início processamento ({VERSAO_SCRIPT}): {caminho_txt.name}")

    content = caminho_txt.read_text(encoding="latin-1", errors="ignore")
    content = content.replace("% CDI", "%CDI").replace("_", "")

    period_d1 = period_d2 = None
    m2 = re.search(r"RECOMPRA\s+(\d{2}/\d{2}/\d{4})\s+(\d{2}/\d{2}/\d{4})", content)
    if m2:
        period_d1, period_d2 = m2.group(1), m2.group(2)
    else:
        m = re.search(r"SALDO EM\s+(\d{2}/\d{2}/\d{4})\s+SALDO EM\s+(\d{2}/\d{2}/\d{4})", content)
        if m:
            period_d1, period_d2 = m.group(1), m.group(2)

    linhas = [ln.rstrip() for ln in content.splitlines()]

    CABECALHOS = [
        "BRB - BANCO DE BRASILIA S.A", "FCB - SISTEMA DE DEPOSITOS", "FCBR76 - EXTRATO DE APLICACOES",
        "APLICACOES EM CDB/RDB", "CONTA CORRENTE", "CLIENTE:", "C/C:", "PERIODO:", "PAGINA:", "EMISSAO:",
        "NUMERO  APLICACAO VENCIMENTO", "NUMERO APLICACAO VENCIMENTO",
        "TAXA/INDICE", "TOTAL", "SALDO CONSOLIDADO", "SALDO EM CDB/RDB", "VALOR APLICADO"
    ]

    def eh_cabecalho(l):
        s = l.strip()
        if not s:
            return True
        if set(s) <= {"-", " ", "_"}:
            return True
        if s.startswith("-") or s.startswith("EM CASO") or s.startswith("O IMPOSTO"):
            return True
        return any(c in s for c in CABECALHOS)

    linhas_validas = [ln.strip() for ln in linhas if not eh_cabecalho(ln)]
    rx_inicio = re.compile(r"^\d+\s+\d{2}/\d{2}/\d{4}\s+\d{2}/\d{2}/\d{4}")

    registros = []
    i = 0
    while i < len(linhas_validas):
        if rx_inicio.match(linhas_validas[i]):
            bloco = [linhas_validas[i]]
            j = i + 1
            while j < len(linhas_validas) and not rx_inicio.match(linhas_validas[j]):
                bloco.append(linhas_validas[j])
                j += 1
            registros.append(bloco)
            i = j
        else:
            i += 1

    if not registros:
        logging.warning(f"Nenhum registro encontrado em {caminho_txt.name}")
        return pd.DataFrame()

    rows = []
    for rec_lines in registros:
        first_line = rec_lines[0]
        all_text = " ".join(rec_lines)

        tfirst = first_line.split()
        if len(tfirst) < 3:
            continue

        numero, aplicacao, vencimento = tfirst[0], tfirst[1], tfirst[2]

        recompra   = "00/00/0000"
        ir         = "0,00"
        iof        = "0,00"
        resg_bruto = "0,00"
        resg_liq   = "0,00"
        rend_per   = "0,00"
        rend_acum  = "0,00"

        taxa = next((t for t in all_text.split() if TAXA_RX.fullmatch(t)), "102,00%CDI")

        if taxa in all_text:
            post = all_text.split(taxa, 1)[1]
            mdate = DATE_RX.search(post)
            post_vals = MONEY_RX.findall(post)
            if mdate and len(post_vals) > 0:
                recompra = mdate.group(0)

        pre_text = all_text.split(taxa, 1)[0] if taxa in all_text else all_text
        pre_monet = MONEY_RX.findall(pre_text)

        valor_aplicado = norm(pre_monet[0] if len(pre_monet) > 0 else "0,00")
        saldo_em_d1    = norm(pre_monet[1] if len(pre_monet) > 1 else "0,00")

        cand2 = norm(pre_monet[2] if len(pre_monet) > 2 else "0,00")
        cand3 = norm(pre_monet[3] if len(pre_monet) > 3 else "0,00")
        cand4 = norm(pre_monet[4] if len(pre_monet) > 4 else "0,00")
        rest  = pre_monet[5:] if len(pre_monet) > 5 else []

        if len(pre_monet) >= 4 and ratio_small(cand2, saldo_em_d1, lim=0.05) and ratio_small(cand3, saldo_em_d1, lim=0.05):
            saldo_em_d2 = "0,00"
            rend_per    = cand2
            rend_acum   = cand3
            extras = [cand4] + rest
        else:
            saldo_em_d2 = cand2
            rend_per    = cand3
            rend_acum   = cand4
            extras = rest

        if len(pre_monet) == 2 and period_d2 and aplicacao == period_d2:
            saldo_em_d2 = saldo_em_d1
            saldo_em_d1 = "0,00"

        if len(extras) >= 1:
            ir = norm(extras[0])
        if len(extras) >= 2:
            resg_bruto = norm(extras[1])

        rc_out = extrair_campos_rc(rec_lines)
        if rc_out:
            rc_rend_per, rc_rend_acum, rc_ir, rc_iof, rc_resg_bruto, rc_resg_liq = rc_out

            if valor_float(rc_rend_per) > 0:   rend_per   = rc_rend_per
            if valor_float(rc_rend_acum) > 0:  rend_acum  = rc_rend_acum
            if valor_float(rc_ir) > 0:         ir         = rc_ir
            if valor_float(rc_iof) > 0:        iof        = rc_iof
            if valor_float(rc_resg_bruto) > 0: resg_bruto = rc_resg_bruto

            if valor_float(rc_resg_liq) > 0:
                resg_liq = rc_resg_liq
            else:
                if valor_float(resg_bruto) > 0 and (valor_float(ir) > 0 or valor_float(iof) > 0):
                    calc_liq = valor_float(resg_bruto) - valor_float(ir) - valor_float(iof)
                    if calc_liq > 0:
                        resg_liq = fmt_brl(calc_liq)

            if recompra == "00/00/0000":
                dt_rc = extrair_recompra_rc(rec_lines)
                if dt_rc:
                    recompra = dt_rc

        if (rc_out is None) and (valor_float(resg_liq) == 0.0):
            recompra = "00/00/0000"

        saldo2_d1 = "0,00"
        saldo2_d2 = "0,00"

        rows.append([
            numero, aplicacao, vencimento,
            valor_aplicado,
            norm(saldo_em_d1), norm(saldo_em_d2),
            norm(rend_per), norm(rend_acum),
            norm(ir), norm(iof), norm(resg_bruto), norm(resg_liq),
            taxa, recompra,
            norm(saldo2_d1), norm(saldo2_d2),
            caminho_txt.name
        ])

    df = pd.DataFrame(rows, columns=[
        "NUMERO", "APLICACAO", "VENCIMENTO", "VALOR_APLICADO",
        "SALDO_EM_D1", "SALDO_EM_D2",
        "REND_BRUTO_PERIODO", "REND_BRUTO_ACUMULADO",
        "IR", "IOF", "RESGATE_BRUTO", "RESGATE_LIQUIDO",
        "TAXA_INDICE", "RECOMPRA",
        "SALDO2_EM_D1", "SALDO2_EM_D2",
        "ARQUIVO_ORIGEM"
    ])

    logging.info(f"{caminho_txt.name} → {len(df)} registros (parseados)")
    return df


# ================= AUDITORIA (NOVO) =================
def extrair_pdf_carteira92(pdf_path: Path) -> dict:
    """
    Extrai números-chave da Carteira 92:
      - Patrimônio Fechamento
      - Disponível c/c
    Estratégia: pegar o último valor monetário grande ANTES do rótulo.
    """
    try:
        with pdfplumber.open(pdf_path) as pdf:
            texto = "\n".join((pg.extract_text() or "") for pg in pdf.pages)

        def last_money_before(label_regex: str, window=220):
            m = re.search(label_regex, texto, flags=re.IGNORECASE)
            if not m:
                return None
            snippet = texto[max(0, m.start() - window): m.start()]
            found = MONEY_BIG_RX.findall(snippet)
            if found:
                return valor_float(found[-1])
            return None

        patrimonio = last_money_before(r"PATRIM[ÔO]NIO\s+FECHAMENTO")
        disponivel_cc = last_money_before(r"Dispon[íi]vel\s+c/c")

        return {
            "pdf": pdf_path.name,
            "patrimonio_fechamento": patrimonio,
            "disponivel_cc": disponivel_cc
        }
    except Exception as e:
        logging.exception(f"Falha ao ler PDF Carteira92: {pdf_path.name} | {e}")
        return {"pdf": pdf_path.name, "patrimonio_fechamento": None, "disponivel_cc": None}

def ler_brbcardtitulos(txt_path: Path) -> pd.DataFrame:
    """
    Lê o BRBCARDTITULOS e retorna dataframe com colunas:
    tipo, produto, valor_float, vencimento
    """
    linhas = txt_path.read_text(encoding="latin-1", errors="ignore").splitlines()
    recs = []
    for ln in linhas:
        ln = ln.strip()
        if not ln or ln.startswith("@"):
            continue
        parts = ln.split(";")
        if len(parts) < 6:
            continue
        recs.append({
            "tipo": parts[0],
            "produto": parts[1],
            "valor_float": valor_float(parts[3]),
            "vencimento": parts[5]
        })
    df = pd.DataFrame(recs)
    df["VENCIMENTO_DT"] = pd.to_datetime(df["vencimento"], errors="coerce")
    return df

def rodar_auditoria(caminho_excel: Path, caminho_txt: Path):
    """
    Concilia:
      - Total CDB (SALDO_EM_D2 do Excel) vs total CDB no TXT
      - Conciliação por vencimento (CDB)
      - Extrai números do PDF Carteira 92 (se existir na entrada)
    """
    try:
        xdf = pd.read_excel(caminho_excel, engine="openpyxl")
        xdf.columns = [str(c).strip().replace("\n", "_").replace(" ", "_").upper() for c in xdf.columns]
        for c in ["SALDO_EM_D2"]:
            if c in xdf.columns:
                xdf[c] = pd.to_numeric(xdf[c], errors="coerce").fillna(0.0)

        if "VENCIMENTO" in xdf.columns:
            xdf["VENCIMENTO_DT"] = pd.to_datetime(xdf["VENCIMENTO"], dayfirst=True, errors="coerce")
        else:
            xdf["VENCIMENTO_DT"] = pd.NaT

        total_cdb_excel = float(xdf["SALDO_EM_D2"].sum()) if "SALDO_EM_D2" in xdf.columns else 0.0
        cdb_by_venc_excel = (
            xdf.dropna(subset=["VENCIMENTO_DT"])
               .groupby(xdf["VENCIMENTO_DT"].dt.date)["SALDO_EM_D2"]
               .sum()
               .reset_index(name="SALDO_D2_EXCEL")
        )

        tdf = ler_brbcardtitulos(caminho_txt)
        df_cdb = tdf[tdf["produto"].astype(str).str.contains("CDB", na=False)].copy()
        total_cdb_txt = float(df_cdb["valor_float"].fillna(0).sum())
        cdb_by_venc_txt = (
            df_cdb.dropna(subset=["VENCIMENTO_DT"])
                  .groupby(df_cdb["VENCIMENTO_DT"].dt.date)["valor_float"]
                  .sum()
                  .reset_index(name="SALDO_TXT")
        )

        recon = pd.merge(cdb_by_venc_excel, cdb_by_venc_txt, on="VENCIMENTO_DT", how="outer")
        recon["SALDO_D2_EXCEL"] = recon["SALDO_D2_EXCEL"].fillna(0.0)
        recon["SALDO_TXT"] = recon["SALDO_TXT"].fillna(0.0)
        recon["DIF"] = recon["SALDO_D2_EXCEL"] - recon["SALDO_TXT"]
        recon["OK"] = recon["DIF"].abs() < 0.01

        dif_total = total_cdb_excel - total_cdb_txt
        ok_total = abs(dif_total) < 0.01
        ok_venc = bool(recon["OK"].all())

        # tenta achar PDF Carteira 92 na entrada
        pdfs = sorted(PASTA_ENTRADA.glob("Carteira_92_*.pdf"))
        carteira_info = extrair_pdf_carteira92(pdfs[0]) if pdfs else {"pdf": "", "patrimonio_fechamento": None, "disponivel_cc": None}

        resumo = pd.DataFrame([
            {"item": "Excel (Total CDB - SALDO_EM_D2)", "valor": total_cdb_excel},
            {"item": "TXT (Total CDB)", "valor": total_cdb_txt},
            {"item": "Diferença Total (Excel - TXT)", "valor": dif_total},
            {"item": "OK Total?", "valor": str(ok_total)},
            {"item": "OK por vencimento?", "valor": str(ok_venc)},
            {"item": "Carteira92 PDF", "valor": carteira_info.get("pdf", "")},
            {"item": "Carteira92 Patrimônio Fechamento", "valor": carteira_info.get("patrimonio_fechamento")},
            {"item": "Carteira92 Disponível c/c", "valor": carteira_info.get("disponivel_cc")},
        ])

        nome_aud = f"auditoria_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
        caminho_aud = PASTA_SAIDA / nome_aud
        with pd.ExcelWriter(caminho_aud, engine="openpyxl") as writer:
            resumo.to_excel(writer, index=False, sheet_name="Resumo")
            recon.to_excel(writer, index=False, sheet_name="Recon_CDB_Venc")
            xdf.to_excel(writer, index=False, sheet_name="Extratos_Unificados")
            tdf.to_excel(writer, index=False, sheet_name="BRBCARDTITULOS_RAW")

        msg = f"AUDITORIA | TotalCDB Excel={total_cdb_excel:.2f} TXT={total_cdb_txt:.2f} DIF={dif_total:.2f} | OK_TOTAL={ok_total} OK_VENC={ok_venc} | Audit={caminho_aud.name}"
        logging.info(msg)
        print("✅ " + msg)

        if (not ok_total or not ok_venc) and TRAVAR_SE_DIVERGIR:
            raise RuntimeError("Auditoria detectou divergência (TRAVAR_SE_DIVERGIR=True).")

        return caminho_aud

    except Exception:
        logging.exception("Falha na auditoria")
        print("⚠️ Falha na auditoria. Verifique os logs.")
        return None


# ================= EXECUÇÃO =================
def main():
    print(f"Iniciando {VERSAO_SCRIPT}")
    candidatos = list(PASTA_ENTRADA.glob("*.txt")) + list(PASTA_ENTRADA.glob("*.TXT"))
    arquivos_entrada = sorted({p.resolve() for p in candidatos})

    if not arquivos_entrada:
        print(f"⚠️ Nenhum arquivo em {PASTA_ENTRADA}. Coloque os .txt lá e rode novamente.")
        logging.warning(f"Sem arquivos na pasta de entrada: {PASTA_ENTRADA}")
        return

    logging.info(f"{len(arquivos_entrada)} arquivo(s) único(s) a processar.")
    dfs = []
    for txt in arquivos_entrada:
        try:
            df = processar_arquivo(txt)
            if not df.empty:
                dfs.append(df)
            else:
                logging.warning(f"Arquivo sem registros parseados: {txt.name}")
        except Exception:
            logging.exception(f"Erro ao processar {txt.name}")

    if not dfs:
        print("⚠️ Nenhum registro processado nos arquivos.")
        return

    df_final = pd.concat(dfs, ignore_index=True).drop_duplicates()

    # Excel padrão: ARQUIVO_ORIGEM por último
    df_excel = df_final.copy()
    cols_num = [
        "VALOR_APLICADO", "SALDO_EM_D1", "SALDO_EM_D2",
        "REND_BRUTO_PERIODO", "REND_BRUTO_ACUMULADO",
        "IR", "IOF", "RESGATE_BRUTO", "RESGATE_LIQUIDO",
        "SALDO2_EM_D1", "SALDO2_EM_D2"
    ]
    for c in cols_num:
        df_excel[c] = df_excel[c].apply(valor_float)

    nome_excel = f"extratos_unificados_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
    caminho_excel = PASTA_SAIDA / nome_excel
    df_excel.to_excel(caminho_excel, index=False)

    # TXT consolidado (mantido)
    data_header = (datetime.now() - timedelta(days=OFFSET_DIAS_CABECALHO)).strftime("%Y-%m-%d")
    data_extra = datetime.now().strftime("%Y-%m-%d")

    caminho_txt_consol = None
    try:
        nome_txt_consol = f"BRBCARDTITULOS-{data_header}.txt"
        caminho_txt_consol = gerar_txt(df_final, nome_txt_consol, data_header, data_extra)
        print(f"✅ Excel gerado: {caminho_excel}")
        print(f"✅ TXT consolidado gerado: {caminho_txt_consol}")
    except Exception:
        logging.exception("Falha ao gerar TXT consolidado BRBCARDTITULOS")
        print("⚠️ Excel gerado, mas houve erro na criação do TXT consolidado. Verifique os logs.")

    # Auditoria (novo)
    if GERAR_AUDITORIA and caminho_txt_consol is not None:
        rodar_auditoria(caminho_excel, caminho_txt_consol)

    logging.info("Processamento finalizado com sucesso")

if __name__ == "__main__":
    main()
