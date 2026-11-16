# Sistema para troca de mensagem instantânea

É uma versão simples de um sistema de mensagens no estilo BBS/IRC: bots fazem login, listam e criam canais, se inscrevem em canais e publicam mensagens neles. Todas as mensagens levam um relógio lógico, os servidores elegem um coordenador entre eles para sincronizar o relógio físico e todos os servidores guardam uma cópia de todos os dados (réplica). Toda a comunicação usa ZeroMQ e tudo roda em containers com Docker Compose.

### Como executar

```bash
docker compose up --build
```

### Containers

| Container | Linguagem | Função |
|-----------|-----------|--------|
| `broker` | Python | Broker Request-Reply (ROUTER na porta 5555 para os clientes, DEALER na 5556 para os servidores) |
| `proxy` | Python | Proxy Pub/Sub (XSUB na porta 5557 para os servidores, XPUB na 5558 para os bots) |
| `referencia` | Python | Serviço de referência (REP na porta 5559): rank, lista de servidores e heartbeat. Só fala com os servidores |
| `servidor1`, `servidor2` | Python | Servidores (REP para as requisições dos bots, PUB para as publicações, REP na porta 5560 para os outros servidores, SUB nos tópicos `servers` e `replica`) |
| `cliente1`, `cliente2` | Java | Bots `bot1` e `bot2` (REQ para as requisições, SUB para receber as publicações) |

## Escolhas feitas

### Linguagens

Python no servidor, no broker, no proxy e no serviço de referência, Java no cliente.

### Serialização: MessagePack

As mensagens são serializadas em binário com MessagePack, que tem biblioteca para Python e
Java e não precisa de arquivo de esquema. Toda mensagem é um mapa com:

- `op`: `login`, `create_channel`, `list_channels` ou `publish`
- `timestamp`: horário do envio em milissegundos (em todas as mensagens)
- `clock`: relógio lógico de quem envia (em todas as mensagens)
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
2. um mapa MessagePack com `channel`, `user` (quem publicou), `message`, `timestamp` (horário em
   que o servidor publicou) e `clock`.

A inscrição nos canais é feita só no bot: o socket SUB recebe as mensagens dos tópicos em que
se inscreveu, e pode se inscrever em vários tópicos na mesma conexão. Para cada publicação
recebida, o bot mostra o canal, quem publicou, a mensagem, o timestamp do envio e o horário em
que recebeu (`recebido`). As publicações são recebidas numa thread separada, para o bot
continuar fazendo as requisições ao mesmo tempo.

### Relógio lógico

Bots, servidores e o serviço de referência têm um contador que começa em 0 junto com o processo:
1. antes de enviar qualquer mensagem, o contador é incrementado e vai no campo `clock`;
2. ao receber uma mensagem, o contador passa a ser o maior valor entre o seu e o `clock` recebido.

No bot, as duas threads (requisições e recebimento das publicações) usam o mesmo contador, por
isso os métodos do relógio são `synchronized`.

### Serviço de referência

O serviço de referência é um container novo que só conversa com os servidores, por REQ/REP
direto na porta 5559 (sem passar pelo broker). As mensagens seguem o mesmo formato (mapa
MessagePack com `op`, `name` do servidor, `timestamp` e `clock`):

| Operação | Quando | Resposta |
|----------|--------|----------|
| `rank` | o servidor inicia | `rank` do servidor. O primeiro recebe 1, o segundo 2, e assim por diante; se o mesmo nome pedir de novo, recebe o mesmo rank (sem repetir nomes) |
| `heartbeat` | a cada 10 mensagens de clientes recebidas | `status: ok` (a hora não vem mais no heartbeat) |
| `list` | logo depois do heartbeat e no início de cada eleição | `servers`: lista com `name` e `rank` dos servidores disponíveis |

Se um servidor ficar 30 segundos sem enviar heartbeat, a referência o tira da lista de
disponíveis. Se ele voltar a enviar, volta para a lista com o mesmo rank.

### Eleição do coordenador

Cada servidor guarda na variável `coordenador` o nome do servidor eleito. A eleição usa o
algoritmo do valentão (bullying) visto em aula: o coordenador é o servidor de **maior rank** que
responde.

1. O servidor pede a lista de servidores à referência e envia `election` para cada servidor de
   rank maior que o seu, pelo socket REP da porta 5560 do outro servidor (o host é o nome do
   container, por exemplo `tcp://servidor2:5560`).
2. Quem recebe `election` responde `status: ok` e começa a sua própria eleição.
3. Se algum servidor de rank maior responder OK, quem pediu a eleição para e espera o aviso do
   vencedor. Se nenhum responder em 2 segundos, ele é o coordenador.
4. O vencedor publica o seu nome no tópico `servers` (pelo mesmo proxy do Pub/Sub), e todos os
   servidores guardam esse nome em `coordenador`.

A eleição acontece quando o servidor inicia (ele ainda não sabe quem é o coordenador) e quando o
coordenador não responde ao pedido de sincronização do relógio.

Para esperar a resposta com limite de tempo, cada requisição a outro servidor usa um socket REQ
novo, que é fechado depois, porque um REQ que ficou sem resposta não consegue enviar outra mensagem.

### Relógio físico: algoritmo de Berkeley

A cada 15 mensagens trocadas com os clientes, o servidor que não é o coordenador envia `sync`
para o coordenador com o seu relógio (o `timestamp` da mensagem) e recebe a hora correta (`time`).

No coordenador, a hora correta é calculada como no algoritmo de Berkeley: ele guarda a diferença
entre o relógio de cada servidor e o seu, faz a média das diferenças (o seu relógio entra com
diferença 0), ajusta o seu próprio relógio para essa média e responde a hora já ajustada.

Como o servidor não pode mudar o relógio do container, ele guarda a diferença (`offset`) entre a
hora correta e o seu relógio, e passa a usar o relógio corrigido em todos os timestamps. O
cálculo considera o tempo de ida e volta da mensagem, como na aula: com T1 = envio e T4 =
recebimento da resposta, `offset = hora_correta + (T4 - T1) / 2 - T4`.

As mensagens entre servidores e as mensagens dos tópicos `servers` e `replica` também levam `timestamp` e `clock`.

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

Com a réplica (seção abaixo), os dois arquivos ficam com os mesmos dados.

### Consistência e réplica dos dados

**Problema:** o broker distribui as requisições entre os servidores com round-robin, então cada
servidor recebia só uma parte dos logins, canais e publicações. Se um servidor parasse, essa parte
do histórico se perdia, e um bot só via os canais criados no servidor que o atendia.

**Método escolhido: réplica ativa com atualização por push**, em vez de copiar o banco inteiro entre os servidores (réplica passiva), cada servidor propaga a **operação de atualização** para os outros, que executam a mesma operação no seu banco.
A atualização é iniciada pelo servidor que recebeu a escrita (push).

**Como foi implementado:**
1. Quando um servidor executa uma operação de escrita de um bot (`login`, `create_channel` ou
   `publish`), ele salva no seu banco, publica a operação no tópico `replica` (pelo mesmo proxy
   do Pub/Sub) e responde o bot. A mensagem leva `op`, `origin` (quem fez a operação), `data` (os
   dados já com os timestamps definidos pelo servidor de origem), `timestamp` e `clock`.
2. Todos os servidores estão inscritos no tópico `replica` e executam a operação recebida no seu
   banco. A própria mensagem também chega para quem publicou e é ignorada. Os bots não recebem
   essas mensagens, porque só se inscrevem nos tópicos dos canais.
3. A réplica de uma publicação só grava no banco: ela não é publicada de novo no canal, senão os
   bots receberiam a mesma mensagem duas vezes.

**Mudanças no método para funcionar no projeto:**
- **Servidor que estava parado:** a propagação por Pub/Sub não guarda mensagens para quem não está
  conectado, então um servidor que ficou parado perderia as operações desse período. Por isso,
  ao iniciar, o servidor também faz uma **atualização por pull**: pede o histórico (`history`) a
  outro servidor disponível, pelo REP da porta 5560, e insere o que estiver faltando. Assim os
  bancos voltam a ficar iguais (consistência eventual: depois que as atualizações param de chegar,
  todas as réplicas têm os mesmos dados).
- **Operações repetidas:** a mesma operação pode chegar pelo tópico e pelo histórico, então antes
  de inserir o servidor confere se aquela linha já existe.
- **Ordem e conflitos:** na réplica ativa clássica todas as réplicas executam as operações na mesma
  ordem. Aqui as operações chegam em servidores diferentes e não há uma ordem global, mas logins e
  publicações não conflitam entre si (cada um é uma linha nova), então a ordem não muda o resultado
  final. O único conflito possível é o mesmo canal ser criado em dois servidores ao mesmo tempo:
  nesse caso, como na consistência eventual forte, os dois ficam com a criação de
  **menor timestamp**, que dá para comparar porque os relógios são sincronizados pelo coordenador
  (Berkeley).

### Exibição das mensagens

Cliente e servidor mostram na tela cada mensagem enviada e recebida, um campo por linha, com o
timestamp também em data e hora (fuso `America/Sao_Paulo`, definido pela variável `TZ` no
`docker-compose.yaml`):

```
[bot1] >>> ENVIADO
    op        : create_channel
    timestamp : 1793937375475 (2026-11-06 00:56:15.475-03:00)
    clock     : 5
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