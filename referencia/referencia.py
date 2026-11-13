import time
from datetime import datetime

import msgpack
import zmq

# Serviço de referência: só conversa com os servidores (nunca com os bots).
# Dá o rank de cada servidor, guarda a lista de servidores e recebe os heartbeats.

# tempo sem heartbeat (em ms) para o servidor sair da lista de disponíveis
TIMEOUT_HEARTBEAT = 30000

relogio = 0             # relógio lógico deste processo
ranks = {}              # nome -> rank (o nome não se repete e o rank não muda)
ultimo_heartbeat = {}   # nome -> horário do último contato (só os servidores disponíveis)


def agora():
    return int(time.time() * 1000)


def formatar_ts(ms):
    try:
        return datetime.fromtimestamp(ms / 1000).astimezone().isoformat(sep=" ", timespec="milliseconds")
    except (OSError, OverflowError, ValueError):
        return "data invalida"


# Mesmo formato de exibição do servidor
def exibir(direcao, msg):
    linhas = [f"[referencia] {direcao}"]
    for chave, valor in msg.items():
        if chave in ("timestamp", "time") and isinstance(valor, int):
            valor = f"{valor} ({formatar_ts(valor)})"
        linhas.append(f"    {chave:<10}: {valor}")
    print("\n".join(linhas), flush=True)


# Relógio lógico: incrementa antes de enviar e, ao receber, fica com o maior valor
def incrementar_relogio():
    global relogio
    relogio += 1
    return relogio


def atualizar_relogio(valor):
    global relogio
    if isinstance(valor, int):
        relogio = max(relogio, valor)


# Rank: o primeiro servidor que pedir recebe 1, o segundo 2 e assim por diante.
# Se o mesmo nome pedir de novo, recebe o mesmo rank (sem repetir o nome na lista).
def pedir_rank(req):
    nome = req.get("name")
    if not nome or not isinstance(nome, str):
        return {"status": "error", "error": "nome do servidor vazio"}
    if nome not in ranks:
        ranks[nome] = len(ranks) + 1
    ultimo_heartbeat[nome] = agora()
    return {"status": "ok", "rank": ranks[nome]}


# Lista com nome e rank dos servidores disponíveis
def listar(req):
    servidores = [{"name": nome, "rank": ranks[nome]} for nome in sorted(ultimo_heartbeat, key=ranks.get)]
    return {"status": "ok", "servers": servidores}


# Heartbeat: o servidor avisa que continua funcionando. A hora correta agora vem do coordenador, então só responde OK
def heartbeat(req):
    nome = req.get("name")
    if nome not in ranks:
        return {"status": "error", "error": "servidor sem rank"}
    ultimo_heartbeat[nome] = agora()
    return {"status": "ok"}


OPERACOES = {
    "rank": pedir_rank,
    "list": listar,
    "heartbeat": heartbeat,
}


# Tira da lista os servidores que pararam de mandar heartbeat
def remover_inativos():
    for nome, ultimo in list(ultimo_heartbeat.items()):
        if agora() - ultimo > TIMEOUT_HEARTBEAT:
            del ultimo_heartbeat[nome]
            print(f"[referencia] {nome} removido da lista (sem heartbeat ha mais de {TIMEOUT_HEARTBEAT // 1000} s)", flush=True)


def tratar(dados):
    try:
        req = msgpack.unpackb(dados)
    except Exception:
        return {"op": "unknown", "status": "error", "error": "mensagem nao esta em MessagePack"}

    atualizar_relogio(req.get("clock"))
    exibir("<<< RECEBIDO", req)
    op = req.get("op")
    if op not in OPERACOES:
        return {"op": op, "status": "error", "error": f"operacao desconhecida: {op}"}
    return {"op": op, **OPERACOES[op](req)}


def main():
    context = zmq.Context()
    # REP: os servidores fazem as requisições com REQ direto nesta porta (sem passar pelo broker)
    socket = context.socket(zmq.REP)
    socket.bind("tcp://*:5559")
    print("Referencia ativa na porta 5559", flush=True)

    # O poll espera no máximo 1 s por uma mensagem, assim dá para verificar os heartbeats mesmo sem requisições
    while True:
        if socket.poll(1000):
            resposta = tratar(socket.recv())
            resposta["timestamp"] = agora()
            resposta["clock"] = incrementar_relogio()
            socket.send(msgpack.packb(resposta))
            exibir(">>> ENVIADO", resposta)
        remover_inativos()


if __name__ == "__main__":
    main()
