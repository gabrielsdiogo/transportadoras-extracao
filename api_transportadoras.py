from flask import Flask, jsonify, request
from flasgger import Swagger
import importlib
import traceback

# ========== CONFIGURAÇÃO BASE ==========
app = Flask(__name__)

swagger_template = {
    "info": {
        "title": "API de Scrapers de Transportadoras",
        "description": """
        Esta API centraliza a execução de múltiplos scrapers de transportadoras brasileiras.
        Cada scraper tem um **ID fixo** e permite:
        - Consultar o número total de páginas disponíveis (`/scripts`)
        - Extrair transportadoras de uma página específica (`/executar?id=...&pagina=...`)
        """,
        "version": "1.0.0",
        "contact": {
            "name": "Gabriel Diogo (Magnata)",
            "email": "gabriel@example.com"
        },
    },
    "schemes": ["http"],
}

swagger = Swagger(app, template=swagger_template)

# ========== MAPA DE SCRIPTS ==========
SCRIPTS = {
    1: {"nome": "Portal dos Fretes", "modulo": "app"},
    2: {"nome": "Cargas.com.br", "modulo": "app2"},
    3: {"nome": "Guia do Transporte", "modulo": "app3"},
}


# ========== ENDPOINT: LISTAR SCRIPTS ==========
@app.route("/scripts", methods=["GET"])
def listar_scripts():
    """
    Lista os scrapers disponíveis e retorna o número total de páginas de cada um.

    ---
    tags:
      - Scrapers
    responses:
      200:
        description: Lista de scrapers disponíveis
        schema:
          type: array
          items:
            type: object
            properties:
              id:
                type: integer
                example: 1
              nome:
                type: string
                example: "Portal dos Fretes"
              total_paginas:
                type: integer
                example: 41
    """
    resultado = []

    for script_id, dados in SCRIPTS.items():
        try:
            modulo = importlib.import_module(dados["modulo"])
            total_paginas = modulo.get_total_paginas()
            resultado.append({
                "id": script_id,
                "nome": dados["nome"],
                "total_paginas": total_paginas
            })
        except Exception as e:
            print(f"⚠️ Erro ao carregar {dados['nome']}: {e}")
            resultado.append({
                "id": script_id,
                "nome": dados["nome"],
                "erro": str(e)
            })

    return jsonify(resultado)


# ========== ENDPOINT: EXECUTAR SCRAPER ==========
@app.route("/executar", methods=["GET"])
def executar_script():
    """
    Executa um scraper específico informando o **ID** e o **número da página**.

    ---
    tags:
      - Scrapers
    parameters:
      - name: id
        in: query
        type: integer
        required: true
        description: ID do scraper (1 = Portal dos Fretes, 2 = Cargas.com.br, 3 = Guia do Transporte)
      - name: pagina
        in: query
        type: integer
        required: true
        description: Número da página a ser extraída
    responses:
      200:
        description: Lista de transportadoras extraídas
        schema:
          type: array
          items:
            type: object
            properties:
              nome:
                type: string
                example: "Zurcad Transportes"
              rotas:
                type: object
                properties:
                  origens:
                    type: array
                    items:
                      type: string
                    example: ["São Paulo - SP"]
                  destinos:
                    type: array
                    items:
                      type: string
                    example: ["Rio de Janeiro - RJ"]
              detalhes:
                type: object
                properties:
                  telefone:
                    type: string
                    example: "(11) 99999-8888"
                  whatsapp:
                    type: string
                    example: "https://wa.me/5511999998888"
                  site:
                    type: string
                    example: "https://zurcadtransportes.com.br"
                  email:
                    type: string
                    example: "contato@zurcad.com.br"
                  endereco:
                    type: string
                    example: "Rua da Liberdade, 123 - São Paulo/SP"
                  cnpj:
                    type: string
                    example: "12.345.678/0001-90"
                  inscricao_estadual:
                    type: string
                    example: "Isento"
    """
    try:
        id_script = int(request.args.get("id", 0))
        pagina = int(request.args.get("pagina", 1))

        if id_script not in SCRIPTS:
            return jsonify({"erro": f"ID {id_script} não encontrado. Use /scripts para listar os disponíveis."}), 400

        script_info = SCRIPTS[id_script]
        modulo = importlib.import_module(script_info["modulo"])

        print(f"🚀 Executando '{script_info['nome']}' | Página {pagina}...")
        resultado = modulo.executar_pagina(pagina)
        print(f"✅ Execução concluída ({script_info['nome']}, página {pagina})")

        return jsonify(resultado)

    except Exception as e:
        traceback.print_exc()
        return jsonify({
            "erro": str(e),
            "detalhes": traceback.format_exc()
        }), 500


# ========== ENDPOINT: HOME ==========
@app.route("/", methods=["GET"])
def home():
    """
    Página inicial da API.
    ---
    tags:
      - Sistema
    responses:
      200:
        description: Status básico da API
        schema:
          type: object
          properties:
            status:
              type: string
              example: "API de Scrapers ativa"
    """
    return jsonify({
        "status": "API de Scrapers ativa",
        "endpoints": ["/scripts", "/executar?id=<id>&pagina=<n>"],
        "swagger_docs": "/apidocs"
    })


# ========== RUN ==========
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050, debug=True)
