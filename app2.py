import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import time

BASE = "https://cargas.com.br"
HEADERS = {"User-Agent": "Mozilla/5.0"}
MAX_WORKERS = 8  # número de threads paralelas


# -------------------------------
# Extrair rotas
# -------------------------------
def extrair_rotas(pagina):
    url = f"{BASE}/rotas?page={pagina}"
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code != 200:
        return []
    soup = BeautifulSoup(resp.text, "html.parser")

    rotas = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/transportadoras/" in href:
            href = unquote(href)
            partes = href.split("/transportadoras/")[-1].split("/")
            if len(partes) == 2:
                origem = partes[0].replace("-", " ").title()
                destino = partes[1].replace("-", " ").title()
                rotas.append({
                    "origem": origem,
                    "destino": destino,
                    "link": urljoin(BASE, href)
                })
    return rotas


# -------------------------------
# Extrair transportadoras por rota
# -------------------------------
def extrair_transportadoras(rota):
    resp = requests.get(rota["link"], headers=HEADERS)
    if resp.status_code != 200:
        return []
    soup = BeautifulSoup(resp.text, "html.parser")

    empresas = []
    for a in soup.find_all("a", href=True):
        if "/transportadora/" in a["href"] and not "/transportadoras/" in a["href"]:
            nome = a.get_text(strip=True)
            link = urljoin(BASE, a["href"])
            if nome:
                empresas.append({
                    "nome": nome,
                    "origem": rota["origem"],
                    "destino": rota["destino"],
                    "link_transportadora": link
                })
    return empresas


# -------------------------------
# Extrair detalhes da transportadora individual
# -------------------------------
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
            return {
                "nome": emp["nome"],
                "rotas": {"origens": [emp["origem"]], "destinos": [emp["destino"]]},
                "detalhes": detalhes
            }

        soup = BeautifulSoup(resp.text, "html.parser")

        # Nome completo
        nome_tag = soup.find("h1")
        nome_real = nome_tag.get_text(strip=True) if nome_tag else emp["nome"]

        # CNPJ e inscrição estadual
        cnpj_ie_tag = soup.select_one("#cargasAbout div div div:nth-of-type(3) div:nth-of-type(1) p:nth-of-type(1)")
        if cnpj_ie_tag:
            texto = cnpj_ie_tag.get_text(" ", strip=True)
            m_cnpj = re.search(r"\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}", texto)
            if m_cnpj:
                detalhes["cnpj"] = m_cnpj.group()
            m_ie = re.search(r"(?:I\.?E\.?|Inscrição\s*Estadual)[:\s]*([A-Za-z0-9./-]+|isento)", texto, re.IGNORECASE)
            if m_ie:
                detalhes["inscricao_estadual"] = m_ie.group(1).strip()

        # Endereço
        endereco_tag = soup.select_one("#cargasAbout div div div:nth-of-type(3) div:nth-of-type(1) p:nth-of-type(2)")
        if endereco_tag:
            detalhes["endereco"] = endereco_tag.get_text(strip=True)

        # Email
        email_tag = soup.select_one("#cargasAbout div div div:nth-of-type(3) div:nth-of-type(2) div:nth-of-type(2) div a[href^='mailto:']")
        if email_tag:
            detalhes["email"] = email_tag["href"].replace("mailto:", "").strip()

        # Telefone
        telefone_tag = soup.select_one("#cargasAbout div div div:nth-of-type(3) div:nth-of-type(2) div:nth-of-type(3) div a[href^='tel:']")
        if telefone_tag:
            detalhes["telefone"] = telefone_tag["href"].replace("tel:", "").strip()
        else:
            m = re.search(r"\(?\d{2}\)?\s*\d{4,5}-?\d{4}", soup.get_text())
            if m:
                detalhes["telefone"] = m.group().strip()

        # Site
        site_tag = soup.select_one("#cargasAbout div div div:nth-of-type(3) div:nth-of-type(2) div:nth-of-type(4) div a[href^='http']")
        if site_tag:
            detalhes["site"] = site_tag["href"]

        # WhatsApp
        ws_tag = soup.find("a", href=lambda h: h and ("wa.me" in h or "whatsapp" in h))
        if ws_tag:
            detalhes["whatsapp"] = ws_tag["href"]

    except Exception as e:
        print(f"⚠️ Erro em {emp.get('nome', 'desconhecido')}: {e}")

    return {
        "nome": nome_real if 'nome_real' in locals() else emp.get("nome", ""),
        "rotas": {
            "origens": [emp.get("origem")] if emp.get("origem") else [],
            "destinos": [emp.get("destino")] if emp.get("destino") else []
        },
        "detalhes": detalhes
    }


# -------------------------------
# 🔹 Função pública: retorna total de páginas
# -------------------------------
def get_total_paginas():
    url = f"{BASE}/rotas?page=1"
    resp = requests.get(url, headers=HEADERS)
    if resp.status_code != 200:
        return 0

    soup = BeautifulSoup(resp.text, "html.parser")
    paginas = []
    for a in soup.select("a[href*='rotas?page=']"):
        href = a.get("href", "")
        m = re.search(r"page=(\d+)", href)
        if m:
            paginas.append(int(m.group(1)))
    return max(paginas) if paginas else 1


# -------------------------------
# 🔹 Função pública: executa scraping de uma página
# -------------------------------
def executar_pagina(pagina_num):
    rotas = extrair_rotas(pagina_num)
    if not rotas:
        return {"mensagem": f"Nenhuma rota encontrada na página {pagina_num}"}

    empresas = []
    for rota in rotas:
        empresas.extend(extrair_transportadoras(rota))

    resultados = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(extrair_detalhes_transportadora, emp) for emp in empresas]
        for future in as_completed(futures):
            data = future.result()
            if data:
                resultados.append(data)

    empresas_map = {}
    for emp in resultados:
        nome = emp["nome"]
        if nome not in empresas_map:
            empresas_map[nome] = {
                "nome": nome,
                "rotas": {"origens": [], "destinos": []},
                "detalhes": emp["detalhes"]
            }
        empresas_map[nome]["rotas"]["origens"].extend(emp["rotas"]["origens"])
        empresas_map[nome]["rotas"]["destinos"].extend(emp["rotas"]["destinos"])

    for emp in empresas_map.values():
        emp["rotas"]["origens"] = list(set(emp["rotas"]["origens"]))
        emp["rotas"]["destinos"] = list(set(emp["rotas"]["destinos"]))

    # Pequena pausa de segurança
    time.sleep(0.3)

    return list(empresas_map.values())

