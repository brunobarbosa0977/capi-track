# CAPI Track — Meta Conversions API

Painel para disparar eventos de conversão direto na Conversions API da Meta (server-side).

## Deploy no Railway (5 minutos)

### 1. Crie uma conta gratuita
Acesse https://railway.app e crie uma conta (pode logar com GitHub).

### 2. Suba o código no GitHub
1. Crie um repositório no GitHub (pode ser privado)
2. Suba todos esses arquivos

```bash
git init
git add .
git commit -m "first commit"
git branch -M main
git remote add origin https://github.com/SEU_USUARIO/SEU_REPO.git
git push -u origin main
```

### 3. Crie o projeto no Railway
1. No Railway, clique em **"New Project"**
2. Escolha **"Deploy from GitHub repo"**
3. Selecione seu repositório
4. Railway detecta automaticamente Node.js e faz o deploy

### 4. Configure a variável de ambiente (opcional)
No Railway → seu projeto → **Variables**:
- `BASE_URL` = a URL que o Railway gerar (ex: `https://seuapp.up.railway.app`)

### 5. Acesse e configure
1. Abra a URL do Railway no navegador
2. Na primeira vez, qualquer senha que você digitar será a senha do sistema
3. Vá em **Configurações** e insira seu Pixel ID e Access Token da Meta
4. Pronto!

---

## Como usar

### Envio Manual
- Aba **Envio** → **Envio Manual**
- Preencha nome, telefone, valor
- Clique em "Enviar para Meta"

### Disparo em Massa
- Aba **Envio** → **Disparo em Massa**
- Faça upload de um CSV ou Excel com as colunas:
  - `nome` (ou `name`)
  - `telefone` (ou `phone`)
  - `email` (opcional)
  - `valor` (ou `value`)
- Veja o preview e clique em "Disparar Todos"

### Webhook (automação via N8n / Make / Manychat)
- Aba **Configurações** → copie o link do webhook
- Envie um POST com body JSON:
```json
{
  "name": "Nome do Lead",
  "phone": "5511999999999",
  "email": "email@exemplo.com",
  "value": 197
}
```

---

## Como gerar o Access Token da Meta
1. Acesse business.facebook.com
2. Vá em **Gerenciador de Eventos**
3. Selecione seu Pixel
4. Clique em **Configurações**
5. Em **API de Conversões**, clique em **Gerar Token de Acesso**
