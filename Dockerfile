# ========================
# Etapa: Build da API Flask
# ========================
FROM python:3.12-slim

# Definir diretório de trabalho
WORKDIR /app

# Evita cache do pip
ENV PIP_NO_CACHE_DIR=1

# Copiar dependências
COPY requirements.txt .

# Instalar dependências
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copiar código da aplicação
COPY . .

# Expor porta padrão da API
EXPOSE 5050

# Rodar a API
CMD ["python", "api_transportadoras.py"]
