import requests
from bs4 import BeautifulSoup
import re
import time
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE = "https://www.guiadotransporte.com.br"
HEADERS = {"User-Agent": "Mozilla/5.0"}
LIMITE_ROTAS = None
MAX_WORKERS = 10

# Variável global para armazenar a rota atual
ROTA_ATUAL = {"origem": None, "destino": None}


# ----------------------------
# Extrai as rotas (origem/destino)
# ----------------------------
def extrair_links_rotas(pagina):
    url = f"{BASE}/cotacao-transportadora/origem-e-destino?page={pagina}"
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code != 200:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    rotas = []

    for a in soup.select("div.grid a[href*='/rotas/']"):
        href = a.get("href")
        if not href:
            continue

        texto = " ".join(a.stripped_strings)
        match = re.search(r"De\s+(.+?)\s+para\s+(.+)", texto, re.IGNORECASE)
        if match:
            origem = match.group(1).strip()
            destino = match.group(2).strip()
        else:
            origem, destino = None, None

        rotas.append({
            "origem": origem,
            "destino": destino,
            "link": urljoin(BASE, href)
        })

    return rotas


# ----------------------------
# Extrai as transportadoras de cada rota
# ----------------------------
def extrair_transportadoras_da_rota(rota):
    global ROTA_ATUAL
    ROTA_ATUAL = rota

    resp = requests.get(rota["link"], headers=HEADERS)
    if resp.status_code != 200:
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    empresas = []
    links_vistos = set()

    for link_tag in soup.select("a[href*='/transportadora/']"):
        href = link_tag.get("href", "")
        if "/transportadora/" not in href:
            continue

        href_full = urljoin(BASE, href)
        if href_full in links_vistos:
            continue
        links_vistos.add(href_full)

        nome = link_tag.get_text(strip=True)
        if not nome:
            h4 = link_tag.find("h4")
            if h4:
                nome = h4.get_text(strip=True)

        empresas.append({
            "nome": nome,
            "origem": rota["origem"],
            "destino": rota["destino"],
            "link_transportadora": href_full
        })

    return empresas


# ----------------------------
# Extrai os detalhes de uma transportadora
# ----------------------------
def extrair_detalhes_transportadora(emp):
    detalhes = {
        "cnpj": None,
        "inscricao_estadual": None,
        "endereco": None,
        "email": None,
        "telefone": None,
        "site": None,
        "whatsapp": None
    }

    try:
        resp = requests.get(emp["link_transportadora"], headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return montar_objeto(emp, detalhes)

        soup = BeautifulSoup(resp.text, "html.parser")

        nome_tag = soup.select_one("body > section:nth-of-type(1) div div:nth-of-type(1) div h3")
        nome_real = nome_tag.get_text(strip=True) if nome_tag else emp["nome"]

        # CNPJ
        texto_html = soup.get_text(" ", strip=True)
        m_cnpj = re.search(r"\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}", texto_html)
        if m_cnpj:
            detalhes["cnpj"] = m_cnpj.group()

        # Endereço
        end_tag = soup.select_one("body section div div:nth-of-type(1) div p")
        if end_tag:
            detalhes["endereco"] = end_tag.get_text(strip=True)

        # Email
        email_tag = soup.select_one("a[href^='mailto:']")
        if email_tag:
            detalhes["email"] = email_tag["href"].replace("mailto:", "").strip()

        # Telefone
        tel_tag = soup.select_one("a[href^='tel:']")
        if tel_tag:
            detalhes["telefone"] = tel_tag["href"].replace("tel:", "").strip()

        # Site
        site_tag = soup.select_one("a[href^='http']")
        if site_tag:
            href_site = site_tag["href"].strip()
            if "guiadotransporte.com.br" not in href_site:
                detalhes["site"] = href_site

        # WhatsApp
        ws_tag = soup.find("a", href=lambda h: h and "wa.me" in h)
        if ws_tag:
            detalhes["whatsapp"] = ws_tag["href"]

        # Fallbacks
        texto_completo = soup.get_text(" ", strip=True)
        if not detalhes["telefone"]:
            tel_match = re.search(r"\(?\d{2}\)?\s?\d{4,5}-?\d{4}", texto_completo)
            if tel_match:
                detalhes["telefone"] = tel_match.group()
        if not detalhes["email"]:
            mail_match = re.search(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", texto_completo)
            if mail_match:
                detalhes["email"] = mail_match.group()

    except Exception as e:
        print(f"⚠️ Erro em {emp['nome']}: {e}")

    return montar_objeto(
        {
            "nome": nome_real,
            "origem": emp.get("origem") or ROTA_ATUAL.get("origem"),
            "destino": emp.get("destino") or ROTA_ATUAL.get("destino")
        },
        detalhes
    )


# ----------------------------
# Monta objeto final
# ----------------------------
def montar_objeto(emp, detalhes):
    return {
        "nome": emp.get("nome", ""),
        "rotas": {
            "origens": [emp.get("origem")] if emp.get("origem") else [],
            "destinos": [emp.get("destino")] if emp.get("destino") else []
        },
        "detalhes": detalhes
    }


# ----------------------------
# 🔹 Retorna total de páginas
# ----------------------------
def get_total_paginas():
    url = f"{BASE}/cotacao-transportadora/origem-e-destino?page=1"
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code != 200:
        return 0

    soup = BeautifulSoup(resp.text, "html.parser")
    paginas = []
    for a in soup.select("a[href*='origem-e-destino?page=']"):
        href = a.get("href", "")
        m = re.search(r"page=(\d+)", href)
        if m:
            paginas.append(int(m.group(1)))
    return max(paginas) if paginas else 1


# ----------------------------
# 🔹 Executa scraping de uma página
# ----------------------------
def executar_pagina(pagina):
    empresas_map = {}
    rotas = extrair_links_rotas(pagina)
    if not rotas:
        return {"mensagem": f"Nenhuma rota encontrada na página {pagina}"}

    for rota in rotas:
        empresas = extrair_transportadoras_da_rota(rota)
        if not empresas:
            continue

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(extrair_detalhes_transportadora, emp): emp for emp in empresas}

            for future in as_completed(futures):
                emp = futures[future]
                try:
                    detalhes_completos = future.result()
                    nome_base = emp["nome"].strip()
                    nome_final = detalhes_completos["nome"].strip()

                    if nome_base not in empresas_map:
                        empresas_map[nome_base] = {
                            "nome": nome_base,
                            "rotas": {"origens": [], "destinos": []},
                            "detalhes": {}
                        }

                    if emp.get("origem") and emp["origem"] not in empresas_map[nome_base]["rotas"]["origens"]:
                        empresas_map[nome_base]["rotas"]["origens"].append(emp["origem"])
                    if emp.get("destino") and emp["destino"] not in empresas_map[nome_base]["rotas"]["destinos"]:
                        empresas_map[nome_base]["rotas"]["destinos"].append(emp["destino"])

                    if nome_final and nome_final != nome_base:
                        empresas_map[nome_final] = empresas_map.pop(nome_base)
                        empresas_map[nome_final]["nome"] = nome_final
                        nome_base = nome_final

                    empresas_map[nome_base]["detalhes"] = detalhes_completos["detalhes"]

                except Exception as e:
                    print(f"⚠️ Erro ao processar {emp.get('nome', 'desconhecido')}: {e}")

        time.sleep(0.3)

    for emp in empresas_map.values():
        emp["rotas"]["origens"] = sorted(list(set(emp["rotas"]["origens"])))
        emp["rotas"]["destinos"] = sorted(list(set(emp["rotas"]["destinos"])))

    return list(empresas_map.values())
