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
session = requests.Session()
session.headers.update(HEADERS)

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
        "whatsapp": None,
        "imagem": None
    }

    nome_real = emp.get("nome", "")
    url = emp["link_transportadora"]

    try:
        # -------------------------------
        # 🔁 Retentativas leves (3x) com tempo curto
        # -------------------------------
        for tentativa in range(3):
            try:
                resp = session.get(url, timeout=15)
                if resp.status_code == 200:
                    break
            except requests.exceptions.RequestException:
                if tentativa < 2:
                    time.sleep(1)
                    continue
                return montar_objeto(emp, detalhes)
        else:
            return montar_objeto(emp, detalhes)

        html = resp.text
        soup = BeautifulSoup(html, "lxml")  # parser mais rápido

        # Nome
        nome_tag = soup.select_one("body > section:nth-of-type(1) div div:nth-of-type(1) div h3")
        if nome_tag:
            nome_real = nome_tag.get_text(strip=True)

        # Imagem
        img_tag = soup.select_one("body > section:nth-of-type(1) div div:nth-of-type(1) > img")
        if not img_tag:
            img_tag = soup.find("img", class_=re.compile(r"bg-guiadamudanca|guiadotransporte"))
        if img_tag and img_tag.has_attr("src"):
            detalhes["imagem"] = urljoin(BASE, img_tag["src"])

        # Texto completo (para buscar tudo em 1 passagem)
        texto = soup.get_text(" ", strip=True)

        # CNPJ
        m_cnpj = re.search(r"\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}", texto)
        if m_cnpj:
            detalhes["cnpj"] = m_cnpj.group()

        # Email
        m_email = re.search(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", texto)
        if m_email:
            detalhes["email"] = m_email.group()

        # Telefone
        m_tel = re.search(r"\(?\d{2}\)?\s?\d{4,5}-?\d{4}", texto)
        if m_tel:
            detalhes["telefone"] = m_tel.group()

        # Endereço (usa heurística simples)
        end_tag = soup.find("p")
        if end_tag and "End" in end_tag.get_text():
            detalhes["endereco"] = end_tag.get_text(strip=True)

        # Site (pega primeiro link externo)
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("http") and "guiadotransporte.com.br" not in href:
                detalhes["site"] = href
                break

        # WhatsApp
        ws_tag = soup.find("a", href=lambda h: h and "wa.me" in h)
        if ws_tag:
            detalhes["whatsapp"] = ws_tag["href"]

    except Exception as e:
        print(f"⚠️ Erro em {emp.get('nome', 'desconhecido')}: {e}")

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
        print(f"\n🌍 Rota: {rota['origem']} → {rota['destino']}")
        empresas = extrair_transportadoras_da_rota(rota)
        if not empresas:
            continue

        for emp in empresas:
            print(f"🔎 {emp['nome']}")
            detalhes_completos = extrair_detalhes_transportadora(emp)

            nome_base = emp["nome"].strip()
            nome_final = detalhes_completos["nome"].strip()

            if nome_base not in empresas_map:
                empresas_map[nome_base] = {
                    "nome": nome_base,
                    "rotas": {"origens": [], "destinos": []},
                    "detalhes": {}
                }

            # Rotas
            if emp.get("origem") and emp["origem"] not in empresas_map[nome_base]["rotas"]["origens"]:
                empresas_map[nome_base]["rotas"]["origens"].append(emp["origem"])
            if emp.get("destino") and emp["destino"] not in empresas_map[nome_base]["rotas"]["destinos"]:
                empresas_map[nome_base]["rotas"]["destinos"].append(emp["destino"])

            # Atualiza nome se necessário
            if nome_final and nome_final != nome_base:
                empresas_map[nome_final] = empresas_map.pop(nome_base)
                empresas_map[nome_final]["nome"] = nome_final
                nome_base = nome_final

            empresas_map[nome_base]["detalhes"] = detalhes_completos["detalhes"]

            # Delay mínimo para estabilidade, mas quase imperceptível
            time.sleep(0.2)

    for emp in empresas_map.values():
        emp["rotas"]["origens"] = sorted(set(emp["rotas"]["origens"]))
        emp["rotas"]["destinos"] = sorted(set(emp["rotas"]["destinos"]))

    return list(empresas_map.values())


if __name__ == "__main__":
    resultados = executar_pagina(1)
    print(resultados)