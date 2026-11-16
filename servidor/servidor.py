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
PROXY_SUB = os.environ.get("PROXY_SUB_ADDR", "tcp://proxy:5558")
REFERENCIA = os.environ.get("REFERENCIA_ADDR", "tcp://referencia:5559")
PORTA_SERVIDORES = os.environ.get("PORTA_SERVIDORES", "5560")
DB_PATH = os.environ.get("DB_PATH", f"/data/{NOME}.db")

# tempo (ms) que espero a resposta de outro servidor antes de considerar que ele não está disponível
TIMEOUT_SERVIDOR = 2000

context = zmq.Context()
# PUB: o servidor publica as mensagens no proxy, usando o nome do canal como tópico
pub = context.socket(zmq.PUB)
# SUB: recebe do proxy os avisos de novo coordenador, no tópico "servers"
sub = context.socket(zmq.SUB)
# REQ: requisições ao serviço de referência (rank, lista de servidores e heartbeat)
ref = context.socket(zmq.REQ)

relogio = 0         # relógio lógico, começa junto com o processo
offset = 0          # diferença (ms) entre o relógio local e a hora do coordenador
rank = None
servidores = []     # última lista de servidores recebida da referência
coordenador = None  # nome do servidor eleito como coordenador
diferencas = {}     # usado só pelo coordenador: nome do servidor -> diferença do relógio dele para o meu


# Timestamp em milissegundos, já corrigido com a hora do coordenador
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
    dados = {"user": usuario, "timestamp": agora()}
    db.execute("INSERT INTO logins VALUES (?, ?)", (usuario, dados["timestamp"]))
    db.commit()
    replicar("login", dados)
    return {"status": "ok"}


# Criação de canal: o nome é chave primária, então se já existir o SQLite recusa e devolve erro de canal duplicado
def criar_canal(db, req):
    canal = req.get("channel")
    if not canal or not isinstance(canal, str):
        return {"status": "error", "error": "nome de canal vazio"}
    dados = {"channel": canal, "user": str(req.get("user")), "timestamp": agora()}
    try:
        db.execute("INSERT INTO canais VALUES (?, ?, ?)", (canal, dados["user"], dados["timestamp"]))
        db.commit()
    except sqlite3.IntegrityError:
        return {"status": "error", "error": f"canal '{canal}' ja existe"}
    replicar("create_channel", dados)
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
    replicar("publish", {"channel": canal, "user": publicacao["user"], "message": mensagem,
                         "timestamp_envio": req["timestamp"], "timestamp_publicacao": publicacao["timestamp"]})
    return {"status": "ok"}


# Réplica ativa com atualização por push: toda operação de escrita que recebe de um cliente é propagada para os outros servidores, que executam a mesma operação no banco deles. A propagação usa o Pub/Sub, no tópico "replica", e leva os dados já com os timestamps definidos por mim, para todos guardarem igual.
def replicar(op, dados):
    msg = {"op": op, "origin": NOME, "data": dados, "timestamp": agora(), "clock": incrementar_relogio()}
    pub.send_multipart([b"replica", msgpack.packb(msg)])
    exibir(">>> PUBLICADO [replica]", msg)


# Executa no banco uma operação que veio de outro servidor (pela réplica ou pelo histórico).
# A mesma operação pode chegar duas vezes (pelo tópico e pelo histórico), então não insiro o que já existe.
def aplicar_operacao(db, op, dados):
    if op == "login":
        db.execute(
            "INSERT INTO logins SELECT ?, ? WHERE NOT EXISTS (SELECT 1 FROM logins WHERE usuario = ? AND timestamp = ?)",
            (dados["user"], dados["timestamp"], dados["user"], dados["timestamp"]),
        )
    elif op == "create_channel":
        # Conflito: o mesmo canal criado em dois servidores ao mesmo tempo. Fica com a criação mais antiga (menor timestamp, que dá para comparar porque os relógios são sincronizados com o coordenador).
        linha = db.execute("SELECT timestamp FROM canais WHERE nome = ?", (dados["channel"],)).fetchone()
        if linha is None:
            db.execute("INSERT INTO canais VALUES (?, ?, ?)", (dados["channel"], dados["user"], dados["timestamp"]))
        elif dados["timestamp"] < linha[0]:
            db.execute("UPDATE canais SET criador = ?, timestamp = ? WHERE nome = ?",
                       (dados["user"], dados["timestamp"], dados["channel"]))
    elif op == "publish":
        db.execute(
            "INSERT INTO publicacoes SELECT ?, ?, ?, ?, ? WHERE NOT EXISTS (SELECT 1 FROM publicacoes "
            "WHERE canal = ? AND usuario = ? AND mensagem = ? AND timestamp_envio = ?)",
            (dados["channel"], dados["user"], dados["message"], dados["timestamp_envio"], dados["timestamp_publicacao"],
             dados["channel"], dados["user"], dados["message"], dados["timestamp_envio"]),
        )
    db.commit()


# Histórico completo do banco, para enviar a um servidor que acabou de iniciar
def historico(db):
    return {
        "status": "ok",
        "logins": [{"user": u, "timestamp": t} for u, t in db.execute("SELECT * FROM logins")],
        "channels": [{"channel": c, "user": u, "timestamp": t} for c, u, t in db.execute("SELECT * FROM canais")],
        "publications": [
            {"channel": c, "user": u, "message": m, "timestamp_envio": te, "timestamp_publicacao": tp}
            for c, u, m, te, tp in db.execute("SELECT * FROM publicacoes")
        ],
    }


# Atualização por pull ao iniciar: o servidor pode ter ficado parado e perdido operações propagadas nesse tempo, então requisita ao histórico para outro servidor disponível e insiro o que estiver faltando.
def buscar_historico(db):
    for servidor in servidores:
        if servidor["name"] == NOME:
            continue
        resposta = requisitar_servidor(servidor["name"], "history")
        if resposta is not None and resposta.get("status") == "ok":
            for dados in resposta["logins"]:
                aplicar_operacao(db, "login", dados)
            for dados in resposta["channels"]:
                aplicar_operacao(db, "create_channel", dados)
            for dados in resposta["publications"]:
                aplicar_operacao(db, "publish", dados)
            print(f"[{NOME}] historico recebido de {servidor['name']}", flush=True)
            return


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


# Heartbeat a cada 10 mensagens de clientes, avisando a referência que continua funcionando.
# A hora agora vem do coordenador, então depois do heartbeat só pega a lista de servidores disponíveis.
def heartbeat():
    global servidores
    requisitar_referencia("heartbeat")
    servidores = requisitar_referencia("list").get("servers", [])


# Endereço do socket que cada servidor usa para falar com os outros servidores (o nome do container é o host)
def endereco(nome):
    return f"tcp://{nome}:{PORTA_SERVIDORES}"


# Requisição para outro servidor (sincronização do relógio ou eleição).
# Usa um REQ novo a cada requisição e espero no máximo TIMEOUT_SERVIDOR: se não vier resposta, o servidor não está disponível e devolve None. O socket é fechado nos dois casos, porque o REQ que ficou sem resposta não consegue enviar de novo.
def requisitar_servidor(nome, op):
    req = {"op": op, "name": NOME, "timestamp": agora(), "clock": incrementar_relogio()}
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)
    socket.connect(endereco(nome))
    socket.send(msgpack.packb(req))
    exibir(f">>> ENVIADO ({nome})", req)

    resposta = None
    if socket.poll(TIMEOUT_SERVIDOR):
        resposta = msgpack.unpackb(socket.recv())
        atualizar_relogio(resposta.get("clock"))
        exibir(f"<<< RECEBIDO ({nome})", resposta)
    else:
        print(f"[{NOME}] {nome} nao respondeu em {TIMEOUT_SERVIDOR} ms", flush=True)
    socket.close()
    return resposta


# Eleição pelo algoritmo do valentão (bullying): pede eleição para todos os servidores de rank maior que o atual.
# Se algum responder OK, ele continua a eleição e depois avisa quem ganhou. Se nenhum responder, eu sou o coordenador.
def eleicao():
    global coordenador, servidores
    print(f"[{NOME}] iniciando eleicao (meu rank: {rank})", flush=True)
    coordenador = None
    servidores = requisitar_referencia("list").get("servers", [])
    for servidor in servidores:
        if servidor["rank"] > rank:
            resposta = requisitar_servidor(servidor["name"], "election")
            if resposta is not None and resposta.get("status") == "ok":
                return
    anunciar_coordenador()


# Quando sou eleito, aviso todos os servidores publicando o meu nome no tópico "servers"
def anunciar_coordenador():
    global coordenador
    coordenador = NOME
    aviso = {"coordinator": NOME, "timestamp": agora(), "clock": incrementar_relogio()}
    pub.send_multipart([b"servers", msgpack.packb(aviso)])
    exibir(">>> PUBLICADO [servers]", aviso)


# Mensagens recebidas pelo SUB:
# - tópico "servers": aviso de novo coordenador, guarda o nome dele;
# - tópico "replica": operação de escrita feita em outro servidor, executa no banco.
#   A própria propagação também chega aqui e é ignorada.
def receber_sub(db):
    global coordenador
    topico, dados = sub.recv_multipart()
    msg = msgpack.unpackb(dados)
    if topico == b"replica" and msg.get("origin") == NOME:
        return
    atualizar_relogio(msg.get("clock"))
    exibir(f"<<< RECEBIDO [{topico.decode()}]", msg)
    if topico == b"servers":
        coordenador = msg.get("coordinator")
        print(f"[{NOME}] coordenador: {coordenador}", flush=True)
    else:
        aplicar_operacao(db, msg["op"], msg["data"])
        print(f"[{NOME}] replica aplicada: {msg['op']} vindo de {msg['origin']}", flush=True)


# Sincronização do relógio a cada 15 mensagens trocadas (algoritmo de Berkeley).
# Envia o relógio (o timestamp da mensagem) para o coordenador, que responde a hora correta. Ajusta o relógio considerando
# o tempo de ida e volta da mensagem: offset = hora_correta + (T4 - T1)/2 - T4.
# Se o coordenador não responder, ele não está disponível e pede uma nova eleição.
def sincronizar_relogio():
    global offset
    if coordenador == NOME:
        return
    if coordenador is None:
        eleicao()
        return
    t1 = time.time() * 1000
    resposta = requisitar_servidor(coordenador, "sync")
    t4 = time.time() * 1000
    if resposta is None or resposta.get("status") != "ok":
        print(f"[{NOME}] coordenador {coordenador} indisponivel", flush=True)
        eleicao()
        return
    offset = int(resposta["time"] + (t4 - t1) / 2 - t4)
    print(f"[{NOME}] relogio fisico ajustado com o coordenador {coordenador}: offset de {offset} ms", flush=True)


# Berkeley no coordenador: guardo a diferença entre o relógio de cada servidor e o meu, e a hora correta é a média dos relógios (entra com diferença 0). Eu também ajusta o relógio para essa média.
def hora_correta(req):
    global offset
    nome = req.get("name")
    diferencas[nome] = req["timestamp"] - agora()
    media = int(sum(diferencas.values()) / (len(diferencas) + 1))
    offset += media
    for outro in diferencas:
        diferencas[outro] -= media  # as diferenças são em relação ao relógio, que mudou
    diferencas[nome] = 0            # o servidor vai acertar o relógio com a hora que responder
    return {"status": "ok", "time": agora()}


# Atende as requisições dos outros servidores: pedido de hora (só o coordenador responde), pedido de eleição e pedido do histórico (de um servidor que acabou de iniciar)
def atender_servidor(socket, db):
    req = msgpack.unpackb(socket.recv())
    atualizar_relogio(req.get("clock"))
    exibir(f"<<< RECEBIDO ({req.get('name')})", req)

    op = req.get("op")
    if op == "sync" and coordenador == NOME:
        resposta = {"op": op, **hora_correta(req)}
    elif op == "sync":
        resposta = {"op": op, "status": "error", "error": "nao sou o coordenador"}
    elif op == "election":
        resposta = {"op": op, "status": "ok"}
    elif op == "history":
        resposta = {"op": op, **historico(db)}
    else:
        resposta = {"op": op, "status": "error", "error": f"operacao desconhecida: {op}"}
    resposta["timestamp"] = agora()
    resposta["clock"] = incrementar_relogio()
    socket.send(msgpack.packb(resposta))
    exibir(f">>> ENVIADO ({req.get('name')})", resposta)

    # Depois de responder OK, o servidor de rank maior continua a eleição
    if op == "election":
        eleicao()


def main():
    global rank
    db = abrir_db()
    # O servidor é o lado REP do Request-Reply e se conecta no DEALER do broker
    socket = context.socket(zmq.REP)
    socket.connect(BROKER)
    # REP para as requisições dos outros servidores (relógio, eleição e histórico)
    servidores_rep = context.socket(zmq.REP)
    servidores_rep.bind(f"tcp://*:{PORTA_SERVIDORES}")
    pub.connect(PROXY)
    sub.connect(PROXY_SUB)
    sub.setsockopt(zmq.SUBSCRIBE, b"servers")
    sub.setsockopt(zmq.SUBSCRIBE, b"replica")
    ref.connect(REFERENCIA)
    print(f"[{NOME}] conectado ao broker {BROKER}, ao proxy {PROXY} e a referencia {REFERENCIA}, dados em {DB_PATH}", flush=True)

    # Ao iniciar, pede o seu rank para a referência
    rank = requisitar_referencia("rank").get("rank")
    print(f"[{NOME}] meu rank: {rank}", flush=True)

    # Espera um pouco para as conexões com o proxy ficarem prontas, senão o aviso de coordenador pode se perder.
    # Ao iniciar ainda não sei quem é o coordenador, então peço uma eleição.
    time.sleep(1)
    eleicao()
    # Pego com outro servidor o que foi feito enquanto eu estava parado
    buscar_historico(db)

    # O servidor espera mensagens em três sockets: clientes (pelo broker), outros servidores e o SUB (tópicos "servers" e "replica")
    poller = zmq.Poller()
    poller.register(socket, zmq.POLLIN)
    poller.register(servidores_rep, zmq.POLLIN)
    poller.register(sub, zmq.POLLIN)

    mensagens_clientes = 0
    while True:
        eventos = dict(poller.poll())
        if sub in eventos:
            receber_sub(db)
        if servidores_rep in eventos:
            atender_servidor(servidores_rep, db)
        if socket in eventos:
            # No REP é sempre recebe -> responde, uma requisição por vez
            resposta = tratar(db, socket.recv())
            resposta["timestamp"] = agora()  # toda mensagem leva o timestamp do envio
            resposta["clock"] = incrementar_relogio()  # e o relógio lógico de quem envia
            socket.send(msgpack.packb(resposta))
            exibir(">>> ENVIADO", resposta)

            mensagens_clientes += 1
            if mensagens_clientes % 10 == 0:
                heartbeat()
            if mensagens_clientes % 15 == 0:
                sincronizar_relogio()


if __name__ == "__main__":
    main()
