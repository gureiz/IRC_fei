# Sistema para troca de mensagem instantânea

É uma versão simples de um sistema de mensagens no estilo BBS/IRC: bots fazem login, listam e criam canais, se inscrevem em canais e publicam mensagens neles. Toda a comunicação usa ZeroMQ e tudo roda em containers com Docker Compose.

### Como executar

```bash
docker compose up --build
```

### Containers

| Container | Linguagem | Função |
|-----------|-----------|--------|
| `broker` | Python | Broker Request-Reply (ROUTER na porta 5555 para os clientes, DEALER na 5556 para os servidores) |
| `proxy` | Python | Proxy Pub/Sub (XSUB na porta 5557 para os servidores, XPUB na 5558 para os bots) |
| `servidor1`, `servidor2` | Python | Servidores (REP para as requisições, PUB para as publicações) |
| `cliente1`, `cliente2` | Java | Bots `bot1` e `bot2` (REQ para as requisições, SUB para receber as publicações) |

## Escolhas feitas

### Linguagens

Python no servidor, no broker e no proxy, Java no cliente.

### Serialização: MessagePack

As mensagens são serializadas em binário com MessagePack, que tem biblioteca para Python e
Java e não precisa de arquivo de esquema. Toda mensagem é um mapa com:

- `op`: `login`, `create_channel`, `list_channels` ou `publish`
- `timestamp`: horário do envio em milissegundos (em todas as mensagens)
- `user`: nome do bot (nas requisições)
- `channel`: nome do canal (na criação de canal e na publicação)
- `message`: texto da mensagem (na publicação)

A resposta tem `status` (`ok` ou `error`), o `timestamp` e, conforme o caso, `error` com a
mensagem de erro ou `channels` com a lista de nomes dos canais.

### Erros

O servidor responde `status: error` quando:
- o nome do usuário ou do canal vem vazio;
- a mensagem de uma publicação vem vazia;
- o canal já existe (canal duplicado não é criado);
- a mensagem não tem `timestamp` ou a operação é desconhecida.

Se o login der erro, o bot repete o login.

### Publicação em canais (Pub/Sub)

O proxy do Pub/Sub é um container separado do broker, baseado no exemplo `src/proxy` da aula:
os servidores publicam no XSUB (porta 5557) e os bots recebem pelo XPUB (porta 5558).

Para publicar, o bot faz uma requisição `publish` ao servidor (pelo broker) com o canal e a
mensagem. O servidor salva a publicação, publica no canal e responde `ok` ou `error`.

A publicação é enviada em duas partes:
1. o tópico, que é o nome do canal;
2. um mapa MessagePack com `channel`, `user` (quem publicou), `message` e `timestamp` (horário em
   que o servidor publicou).

A inscrição nos canais é feita só no bot: o socket SUB recebe as mensagens dos tópicos em que
se inscreveu, e pode se inscrever em vários tópicos na mesma conexão. Para cada publicação
recebida, o bot mostra o canal, quem publicou, a mensagem, o timestamp do envio e o horário em
que recebeu (`recebido`). As publicações são recebidas numa thread separada, para o bot
continuar fazendo as requisições ao mesmo tempo.

### Comportamento do bot

1. Faz login.
2. Se houver menos de 5 canais, cria um canal com um nome de uma lista fixa que ainda não existe
   (se o servidor responder erro, tenta outro nome).
3. Se estiver inscrito em menos de 3 canais, se inscreve em mais um, sorteado da lista.
4. Fica em loop infinito: escolhe um canal da lista de canais e envia 10 mensagens sorteadas
   de uma lista fixa, com 1 segundo de intervalo entre elas.

### Persistência: SQLite

Cada servidor tem o seu próprio arquivo SQLite (`./dados/servidor1/` e `./dados/servidor2/`),
que não é compartilhado entre eles. Ele guarda:
- `logins`: usuário e timestamp de cada login;
- `canais`: nome, usuário que criou e timestamp;
- `publicacoes`: canal, usuário, mensagem, timestamp do envio da requisição pelo bot e timestamp
  da publicação pelo servidor. Com isso dá para recuperar todas as publicações depois.

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