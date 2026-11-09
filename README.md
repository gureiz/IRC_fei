# Sistema para troca de mensagem instantânea

É uma versão simples de um sistema de mensagens no estilo BBS/IRC: bots fazem login, listam e criam canais. Toda a comunicação usa ZeroMQ e tudo roda em containers com Docker Compose.

## Parte 1: login e criação de canais

### Como executar

```bash
docker compose up --build
```

### Containers

| Container | Linguagem | Função |
|-----------|-----------|--------|
| `broker` | Python | Broker Request-Reply (ROUTER na porta 5555 para os clientes, DEALER na 5556 para os servidores) |
| `servidor1`, `servidor2` | Python | Servidores (REP) |
| `cliente1`, `cliente2` | Java | Bots `bot1` e `bot2` (REQ) |

## Escolhas feitas

### Linguagens

Python no servidor e no broker, Java no cliente.

### Serialização: MessagePack

As mensagens são serializadas em binário com MessagePack, que tem biblioteca para Python e
Java e não precisa de arquivo de esquema. Toda mensagem é um mapa com:

- `op`: `login`, `create_channel` ou `list_channels`
- `timestamp`: horário do envio em milissegundos (em todas as mensagens)
- `user`: nome do bot (nas requisições)
- `channel`: nome do canal (na criação de canal)

A resposta tem `status` (`ok` ou `error`), o `timestamp` e, conforme o caso, `error` com a
mensagem de erro ou `channels` com a lista de nomes dos canais.

### Erros

O servidor responde `status: error` quando:
- o nome do usuário ou do canal vem vazio;
- o canal já existe (canal duplicado não é criado);
- a mensagem não tem `timestamp` ou a operação é desconhecida.

Se o login der erro, o bot repete o login.

### Comportamento do bot

1. Faz login.
2. Pede a lista de canais. Se houver menos de 5, cria um canal com um nome de uma lista fixa
   que ainda não existe (se o servidor responder erro, tenta outro nome).
3. Depois fica pedindo a lista de canais a cada 5 segundos.

### Persistência: SQLite

Cada servidor tem o seu próprio arquivo SQLite (`./dados/servidor1/` e `./dados/servidor2/`),
que não é compartilhado entre eles. Ele guarda:
- `logins`: usuário e timestamp de cada login;
- `canais`: nome, usuário que criou e timestamp.

Como cada servidor guarda só os seus dados, um canal só aparece para quem for atendido pelo
servidor que o criou.

### Exibição das mensagens

Cliente e servidor mostram na tela cada mensagem enviada e recebida, um campo por linha, com o
timestamp também em data e hora (fuso `America/Sao_Paulo`, definido pela variável `TZ` no
`docker-compose.yaml`):

```
[bot1] >>> ENVIADO
    op        : create_channel
    timestamp : 1793937375475 (2026-11-06 00:56:15.475-03:00)
    user      : bot1
    channel   : musica
```

### Arquivo `cliente/pom.xml`

É o arquivo de configuração do Maven, a ferramenta que compila o cliente Java. Ele define:
- as bibliotecas usadas pelo bot: `jeromq` (ZeroMQ para Java) e `msgpack-core` (MessagePack);
- a versão do Java (17);
- a geração de um único `bot.jar` com o código e as bibliotecas, tendo `bbs.Bot` como classe principal.

O `cliente/Dockerfile` usa esse arquivo para compilar o bot com `mvn package` e depois executa
o `bot.jar`. O XML é só configuração de build: as mensagens trocadas entre cliente e servidor
são binárias (MessagePack), sem XML.

# Autor
## Gustavo Reis Teixeira RA: 22.124.055-9