import os
import sqlite3
import time
from datetime import datetime

import msgpack
import zmq

# Configurações vindas do docker-compose (cada servidor tem seu nome e seu próprio banco)
NOME = os.environ.get("SERVER_NAME", "servidor")
BROKER = os.environ.get("BROKER_ADDR", "tcp://broker:5556")
PROXY = os.environ.get("PROXY_ADDR", "tcp://proxy:5557")
REFERENCIA = os.environ.get("REFERENCIA_ADDR", "tcp://referencia:5559")
DB_PATH = os.environ.get("DB_PATH", f"/data/{NOME}.db")

context = zmq.Context()
# PUB: o servidor publica as mensagens no proxy, usando o nome do canal como tópico
pub = context.socket(zmq.PUB)
# REQ: requisições ao serviço de referência (rank, lista de servidores e heartbeat)
ref = context.socket(zmq.REQ)

relogio = 0       # relógio lógico, começa junto com o processo
offset = 0        # diferença (ms) entre o relógio local e a hora correta da referência
rank = None
servidores = []   # última lista de servidores recebida da referência


# Timestamp em milissegundos, já corrigido com a hora da referência
def agora():
    return int(time.time() * 1000) + offset


# Relógio lógico: incrementa antes de enviar cada mensagem
def incrementar_relogio():
    global relogio
    relogio += 1
    return relogio


# Ao receber uma mensagem, fica com o maior valor entre o seu contador e o que veio na mensagem
def atualizar_relogio(valor):
    global relogio
    if isinstance(valor, int):
        relogio = max(relogio, valor)


# converte o timestamp para data/hora no fuso do container
def formatar_ts(ms):
    try:
        return datetime.fromtimestamp(ms / 1000).astimezone().isoformat(sep=" ", timespec="milliseconds")
    except (OSError, OverflowError, ValueError):
        return "data invalida"


# Mostra na tela toda mensagem enviada/recebida, um campo por linha, para ficar fácil de acompanhar a troca de mensagens no log do docker compose
def exibir(direcao, msg):
    linhas = [f"[{NOME}] {direcao}"]
    if isinstance(msg, dict):
        for chave, valor in msg.items():
            if chave in ("timestamp", "time") and isinstance(valor, int):
                valor = f"{valor} ({formatar_ts(valor)})"
            linhas.append(f"    {chave:<10}: {valor}")
    else:
        linhas.append(f"    {msg!r}")
    print("\n".join(linhas), flush=True)


# Persistência: cada servidor usa o seu arquivo SQLite, que fica no volume montado em /data.
# O container pode ser recriado que os dados continuam no disco.
def abrir_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    db = sqlite3.connect(DB_PATH)
    db.execute("CREATE TABLE IF NOT EXISTS logins (usuario TEXT NOT NULL, timestamp INTEGER NOT NULL)")
    db.execute(
        "CREATE TABLE IF NOT EXISTS canais ("
        "nome TEXT PRIMARY KEY, criador TEXT NOT NULL, timestamp INTEGER NOT NULL)"
    )
    db.execute(
        "CREATE TABLE IF NOT EXISTS publicacoes ("
        "canal TEXT NOT NULL, usuario TEXT NOT NULL, mensagem TEXT NOT NULL, "
        "timestamp_envio INTEGER NOT NULL, timestamp_publicacao INTEGER NOT NULL)"
    )
    db.commit()
    return db


# Login: sem senha, só confere se veio um nome e salva o login com o horário em que aconteceu
def login(db, req):
    usuario = req.get("user")
    if not usuario or not isinstance(usuario, str):
        return {"status": "error", "error": "nome de usuario vazio"}
    db.execute("INSERT INTO logins VALUES (?, ?)", (usuario, agora()))
    db.commit()
    return {"status": "ok"}


# Criação de canal: o nome é chave primária, então se já existir o SQLite recusa e devolve erro de canal duplicado
def criar_canal(db, req):
    canal = req.get("channel")
    if not canal or not isinstance(canal, str):
        return {"status": "error", "error": "nome de canal vazio"}
    try:
        db.execute("INSERT INTO canais VALUES (?, ?, ?)", (canal, str(req.get("user")), agora()))
        db.commit()
    except sqlite3.IntegrityError:
        return {"status": "error", "error": f"canal '{canal}' ja existe"}
    return {"status": "ok"}


# Listagem: devolve só os nomes dos canais, em ordem alfabética
def listar_canais(db, req):
    canais = [linha[0] for linha in db.execute("SELECT nome FROM canais ORDER BY nome")]
    return {"status": "ok", "channels": canais}


# Publicação: salva no banco e publica no canal (tópico = nome do canal).
def publicar(db, req):
    canal = req.get("channel")
    mensagem = req.get("message")
    if not canal or not isinstance(canal, str):
        return {"status": "error", "error": "nome de canal vazio"}
    if not mensagem or not isinstance(mensagem, str):
        return {"status": "error", "error": "mensagem vazia"}

    publicacao = {"channel": canal, "user": str(req.get("user")), "message": mensagem,
                  "timestamp": agora(), "clock": incrementar_relogio()}
    db.execute(
        "INSERT INTO publicacoes VALUES (?, ?, ?, ?, ?)",
        (canal, publicacao["user"], mensagem, req["timestamp"], publicacao["timestamp"]),
    )
    db.commit()

    # a publicação vai em duas partes: o tópico e a mensagem serializada em MessagePack
    pub.send_multipart([canal.encode(), msgpack.packb(publicacao)])
    exibir(f">>> PUBLICADO [{canal}]", publicacao)
    return {"status": "ok"}


# Cada valor do campo "op" da mensagem chama uma função
OPERACOES = {
    "login": login,
    "create_channel": criar_canal,
    "list_channels": listar_canais,
    "publish": publicar,
}


# Desserializa a requisição (MessagePack). Qualquer problema vira uma resposta com status "error".
def tratar(db, dados):
    try:
        req = msgpack.unpackb(dados)
    except Exception:
        exibir("<<< RECEBIDO (invalido)", dados)
        return {"op": "unknown", "status": "error", "error": "mensagem nao esta em MessagePack"}

    if not isinstance(req, dict):
        exibir("<<< RECEBIDO (invalido)", req)
        return {"op": "unknown", "status": "error", "error": "mensagem deve ser um mapa"}
    atualizar_relogio(req.get("clock"))
    exibir("<<< RECEBIDO", req)

    op = req.get("op")
    if not isinstance(req.get("timestamp"), int):
        resultado = {"status": "error", "error": "mensagem sem timestamp"}
    elif op not in OPERACOES:
        resultado = {"status": "error", "error": f"operacao desconhecida: {op}"}
    else:
        resultado = OPERACOES[op](db, req)
    return {"op": op, **resultado}


# Requisição para o serviço de referência: toda mensagem leva o nome, o timestamp e o relógio lógico
def requisitar_referencia(op):
    req = {"op": op, "name": NOME, "timestamp": agora(), "clock": incrementar_relogio()}
    ref.send(msgpack.packb(req))
    exibir(">>> ENVIADO (referencia)", req)
    resposta = msgpack.unpackb(ref.recv())
    atualizar_relogio(resposta.get("clock"))
    exibir("<<< RECEBIDO (referencia)", resposta)
    return resposta


# Heartbeat a cada 10 mensagens de clientes. Na resposta vem a hora correta, então é ajustado no relógio considerando o tempo de ida e volta da mensagem: offset = hora_ref + (T4 - T1)/2 - T4
def heartbeat():
    global offset, servidores
    t1 = time.time() * 1000
    resposta = requisitar_referencia("heartbeat")
    t4 = time.time() * 1000
    if resposta.get("status") == "ok":
        offset = int(resposta["time"] + (t4 - t1) / 2 - t4)
        print(f"[{NOME}] relogio fisico ajustado: offset de {offset} ms em relacao ao relogio local", flush=True)
    servidores = requisitar_referencia("list").get("servers", [])


def main():
    global rank
    db = abrir_db()
    # O servidor é o lado REP do Request-Reply e se conecta no DEALER do broker
    socket = context.socket(zmq.REP)
    socket.connect(BROKER)
    pub.connect(PROXY)
    ref.connect(REFERENCIA)
    print(f"[{NOME}] conectado ao broker {BROKER}, ao proxy {PROXY} e a referencia {REFERENCIA}, dados em {DB_PATH}", flush=True)

    # Ao iniciar, pede o seu rank para a referência
    rank = requisitar_referencia("rank").get("rank")
    print(f"[{NOME}] meu rank: {rank}", flush=True)

    # No REP é sempre recebe -> responde, uma requisição por vez
    mensagens_clientes = 0
    while True:
        dados = socket.recv()
        resposta = tratar(db, dados)
        resposta["timestamp"] = agora()  # toda mensagem leva o timestamp do envio
        resposta["clock"] = incrementar_relogio()  # e o relógio lógico de quem envia
        socket.send(msgpack.packb(resposta))
        exibir(">>> ENVIADO", resposta)

        mensagens_clientes += 1
        if mensagens_clientes % 10 == 0:
            heartbeat()


if __name__ == "__main__":
    main()
