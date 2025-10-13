import requests
from bs4 import BeautifulSoup
import time
from urllib.parse import urljoin

BASE = "https://portaldosfretes.com.br"


# ---------------------------
# Decodificação do email Cloudflare
# ---------------------------
def decode_cfemail(cfemail):
    try:
        r = int(cfemail[:2], 16)
        email = ''.join(
            [chr(int(cfemail[i:i+2], 16) ^ r) for i in range(2, len(cfemail), 2)]
        )
        return email
    except Exception:
        return None


# ---------------------------
# Extrair links de rotas
# ---------------------------
def extrair_links_rotas(pagina):
    url = f"{BASE}/rotas/pagina-{pagina}"
    resp = requests.get(url)
    if resp.status_code != 200:
        return []
    soup = BeautifulSoup(resp.text, "html.parser")
    links = []
    for a in soup.select("a"):
        href = a.get("href")
        texto = a.get_text(strip=True)
        if href and texto.startswith(("Fretes de", "Frete de")):
            links.append(urljoin(BASE, href))
    return links


# ---------------------------
# Descobrir número total de páginas
# ---------------------------
def get_total_paginas():
    url = f"{BASE}/rotas/pagina-1"
    resp = requests.get(url)
    if resp.status_code != 200:
        return 0
    soup = BeautifulSoup(resp.text, "html.parser")

    paginas = []
    for a in soup.select("a[href*='/rotas/pagina-']"):
        href = a.get("href", "")
        if "pagina-" in href:
            try:
                num = int(href.split("pagina-")[-1].split("/")[0])
                paginas.append(num)
            except ValueError:
                pass
    return max(paginas) if paginas else 1


# ---------------------------
# Parsear origem/destino
# ---------------------------
def parse_rota_nome(rota_url):
    rota = rota_url.split("/")[-1]
    if rota.startswith("frete-de-"):
        rota = rota.replace("frete-de-", "")
    partes = rota.split("-para-")
    if len(partes) == 2:
        origem = partes[0].replace("-", " ").title()
        destino = partes[1].replace("-", " ").title()
        return origem, destino
    return None, None


# ---------------------------
# Extrair empresas da rota
# ---------------------------
def extrair_empresas_da_rota(rota_url):
    resp = requests.get(rota_url)
    if resp.status_code != 200:
        return []
    soup = BeautifulSoup(resp.text, "html.parser")
    empresas = []

    origem, destino = parse_rota_nome(rota_url)

    for bloco in soup.find_all("a", href=lambda h: h and "/transportadora/" in h):
        nome = bloco.get_text(strip=True)

        if not nome:
            card = bloco.find_parent("div", attrs={"data-nome": True})
            if card and card["data-nome"].strip():
                nome = card["data-nome"].strip()
            else:
                p_tag = bloco.find_next("p", class_="blue")
                if p_tag:
                    nome = p_tag.get_text(strip=True)

        link = urljoin(BASE, bloco.get("href"))

        empresas.append({
            "nome": nome,
            "rota_origem": origem,
            "rota_destino": destino,
            "link_transportadora": link
        })
    return empresas


# ---------------------------
# Extrair detalhes da transportadora
# ---------------------------
def extrair_detalhes_transportadora(url_transp):
    resp = requests.get(url_transp)
    if resp.status_code != 200:
        return {}
    soup = BeautifulSoup(resp.text, "html.parser")
    detalhes = {}

    tel_tag = soup.find("a", href=lambda h: h and h.startswith("tel:"))
    if tel_tag:
        detalhes["telefone"] = tel_tag.get_text(strip=True).replace("Telefone:", "").strip()

    ws_tag = soup.find("a", href=lambda h: h and ("wa.me" in h or "whatsapp" in h))
    if ws_tag:
        detalhes["whatsapp"] = ws_tag.get("href")

    site_tag = soup.find("a", href=lambda h: h and "http" in h and "portaldosfretes" not in h)
    if site_tag:
        detalhes["site"] = site_tag.get("href")

    email_span = soup.select_one("span.__cf_email__")
    if email_span and email_span.has_attr("data-cfemail"):
        detalhes["email"] = decode_cfemail(email_span["data-cfemail"])

    for p in soup.find_all("p"):
        txt = p.get_text(strip=True)
        if "Endereço" in txt:
            detalhes["endereco"] = txt.replace("Endereço:", "").strip()
        if "CNPJ" in txt:
            detalhes["cnpj"] = txt.replace("CNPJ:", "").strip()
        if "Inscrição" in txt or "I.E" in txt:
            detalhes["inscricao_estadual"] = txt.replace("Inscrição estadual:", "").replace("I.E:", "").strip()
        if "ANTT" in txt:
            detalhes["antt"] = txt.replace("Número da ANTT:", "").replace("ANTT:", "").strip()

    return detalhes


# ---------------------------
# Função pública chamada pela API central
# ---------------------------
def executar_pagina(pagina_num):
    empresas_map = {}
    rotas = extrair_links_rotas(pagina_num)
    for rota in rotas:
        empresas = extrair_empresas_da_rota(rota)
        for emp in empresas:
            nome = emp["nome"]
            if nome not in empresas_map:
                empresas_map[nome] = {
                    "nome": nome,
                    "rotas": {"origens": [], "destinos": []},
                    "detalhes": {}
                }
            if emp["rota_origem"]:
                empresas_map[nome]["rotas"]["origens"].append(emp["rota_origem"])
            if emp["rota_destino"]:
                empresas_map[nome]["rotas"]["destinos"].append(emp["rota_destino"])
            if not empresas_map[nome]["detalhes"] and emp["link_transportadora"]:
                empresas_map[nome]["detalhes"] = extrair_detalhes_transportadora(emp["link_transportadora"])
        time.sleep(0.5)

    for emp in empresas_map.values():
        emp["rotas"]["origens"] = list(set(emp["rotas"]["origens"]))
        emp["rotas"]["destinos"] = list(set(emp["rotas"]["destinos"]))
    return list(empresas_map.values())
