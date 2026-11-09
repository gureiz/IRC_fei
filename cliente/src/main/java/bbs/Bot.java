package bbs;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.Value;
import org.msgpack.value.ValueFactory;
import org.zeromq.SocketType;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;

// Bot (cliente) do projeto. Roda sozinho, sem interação: faz login, lista e cria canais, se inscreve em canais e publica mensagens.
// Fala com os servidores pelo broker (Request-Reply) e recebe as publicações pelo proxy (Pub/Sub).
public class Bot {
    static final String BROKER = System.getenv().getOrDefault("BROKER_ADDR", "tcp://broker:5555");
    static final String PROXY = System.getenv().getOrDefault("PROXY_ADDR", "tcp://proxy:5558");
    static final int MIN_CANAIS = 5;
    static final int MIN_INSCRICOES = 3;
    // Mensagens que o bot sorteia para publicar
    static final String[] MENSAGENS = {
        "Bom dia, pessoal!", "Alguem viu o jogo ontem?", "Qual a boa de hoje?",
        "Recomendo muito esse livro", "Que calor hoje", "Testando o canal",
        "Alguma novidade?", "Ate mais!", "Concordo totalmente", "Boa noite a todos"
    };
    // Nomes que o bot sorteia quando vai criar um canal
    static final String[] NOMES_CANAIS = {
        "geral", "noticias", "esportes", "musica", "filmes",
        "jogos", "tecnologia", "ciencia", "livros", "viagens"
    };
    // Formato só para exibir o timestamp na tela, no fuso do container
    static final DateTimeFormatter FORMATO_TS =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSXXX").withZone(ZoneId.systemDefault());

    final String usuario;
    final ZContext context = new ZContext();
    final Random random = new Random();
    final List<String> inscritos = new ArrayList<>();
    ZMQ.Socket socket;
    ZMQ.Socket sub;

    Bot(String usuario) {
        this.usuario = usuario;
    }

    public static void main(String[] args) throws Exception {
        // O nome do bot vem do docker-compose (bot1, bot2)
        new Bot(System.getenv().getOrDefault("BOT_NAME", "bot")).executar();
    }

    void executar() throws Exception {
        conectar();

        // Login: uma mensagem de ida e uma de volta. Se der erro, repete o login.
        while (!ok(requisitar("login", Map.of()))) {
            Thread.sleep(1000);
        }

        // 1. se tiver menos de 5 canais, cria um novo com um nome que ainda não está na lista. Como cada servidor tem os seus dados, o servidor pode responder que o canal já existe; nesse caso tenta outro nome.
        List<String> canais = listarCanais();
        if (canais.size() < MIN_CANAIS) {
            List<String> candidatos = new ArrayList<>(List.of(NOMES_CANAIS));
            candidatos.removeAll(canais);
            Collections.shuffle(candidatos, random);
            for (String canal : candidatos) {
                if (ok(requisitar("create_channel", Map.of("channel", canal)))) {
                    canais.add(canal);
                    break;
                }
            }
        }

        // 2. se estiver inscrito em menos de 3 canais, se inscreve em mais um, sorteado da lista
        if (inscritos.size() < MIN_INSCRICOES) {
            List<String> candidatos = new ArrayList<>(canais);
            candidatos.removeAll(inscritos);
            if (!candidatos.isEmpty()) {
                inscrever(candidatos.get(random.nextInt(candidatos.size())));
            }
        }

        // Thread separada só para receber as publicações, enquanto a principal continua fazendo as requisições
        new Thread(this::receberPublicacoes).start();

        // 3. loop infinito: escolhe um canal da lista e envia 10 mensagens aleatórias, uma por segundo
        while (true) {
            canais = listarCanais();
            if (canais.isEmpty()) {
                Thread.sleep(1000);
                continue;
            }
            String canal = canais.get(random.nextInt(canais.size()));
            for (int i = 0; i < 10; i++) {
                Map<String, String> campos = new LinkedHashMap<>();
                campos.put("channel", canal);
                campos.put("message", MENSAGENS[random.nextInt(MENSAGENS.length)]);
                requisitar("publish", campos);
                Thread.sleep(1000);
            }
        }
    }

    // A inscrição é feita só no cliente: o SUB passa a receber as mensagens cujo tópico é o nome do canal.
    // Dá para chamar várias vezes no mesmo socket, uma para cada canal.
    void inscrever(String canal) {
        sub.subscribe(canal.getBytes(ZMQ.CHARSET));
        inscritos.add(canal);
        System.out.println("[" + usuario + "] inscrito no canal " + canal + " (inscricoes: " + inscritos + ")");
    }

    // Cada publicação chega em duas partes: o tópico (nome do canal) e a mensagem em MessagePack.
    // É mostrado o canal, a mensagem, o timestamp do envio e o horário em que foi recebido
    void receberPublicacoes() {
        while (true) {
            String canal = sub.recvStr();
            byte[] dados = sub.recv();
            try {
                Map<String, Value> msg = desserializar(dados);
                msg.put("recebido", ValueFactory.newInteger(System.currentTimeMillis()));
                exibir("<<< PUBLICACAO RECEBIDA [" + canal + "]", msg);
            } catch (IOException e) {
                System.out.println("[" + usuario + "] publicacao invalida no canal " + canal);
            }
        }
    }

    // Pede a lista de canais e converte o array do MessagePack em lista de String
    List<String> listarCanais() throws IOException {
        List<String> canais = new ArrayList<>();
        Map<String, Value> resposta = requisitar("list_channels", Map.of());
        if (ok(resposta)) {
            for (Value v : resposta.get("channels").asArrayValue()) {
                canais.add(v.asStringValue().asString());
            }
        }
        return canais;
    }

    // O bot é o lado REQ e se conecta no ROUTER do broker; o SUB se conecta no XPUB do proxy
    void conectar() {
        socket = context.createSocket(SocketType.REQ);
        socket.connect(BROKER);
        sub = context.createSocket(SocketType.SUB);
        sub.connect(PROXY);
    }

    // Monta a requisição, serializa, envia e espera a resposta (o REQ sempre envia e depois recebe).
    // Se o servidor ainda não subiu, o recv fica esperando e o login acontece assim que o serviço estiver disponível.
    Map<String, Value> requisitar(String op, Map<String, String> campos) throws IOException {
        // operação, timestamp do envio e usuário
        Map<String, Value> msg = new LinkedHashMap<>();
        msg.put("op", ValueFactory.newString(op));
        msg.put("timestamp", ValueFactory.newInteger(System.currentTimeMillis()));
        msg.put("user", ValueFactory.newString(usuario));
        campos.forEach((k, v) -> msg.put(k, ValueFactory.newString(v)));

        socket.send(serializar(msg));
        exibir(">>> ENVIADO", msg);

        Map<String, Value> resposta = desserializar(socket.recv());
        exibir("<<< RECEBIDO", resposta);
        return resposta;
    }

    static boolean ok(Map<String, Value> resposta) {
        return "ok".equals(resposta.get("status").asStringValue().asString());
    }

    // Serialização em binário com MessagePack.
    static byte[] serializar(Map<String, Value> msg) throws IOException {
        MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
        packer.packMapHeader(msg.size());
        for (Map.Entry<String, Value> e : msg.entrySet()) {
            packer.packString(e.getKey());
            packer.packValue(e.getValue());
        }
        packer.close();
        return packer.toByteArray();
    }

    // bytes recebidos -> mapa com os campos da resposta
    static Map<String, Value> desserializar(byte[] dados) throws IOException {
        Map<String, Value> msg = new LinkedHashMap<>();
        try (MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(dados)) {
            for (Map.Entry<Value, Value> e : unpacker.unpackValue().asMapValue().entrySet()) {
                msg.put(e.getKey().asStringValue().asString(), e.getValue());
            }
        }
        return msg;
    }

    // Mostra a mensagem inteira na tela, um campo por linha, no mesmo formato do servidor
    void exibir(String direcao, Map<String, Value> msg) {
        List<String> linhas = new ArrayList<>();
        linhas.add("[" + usuario + "] " + direcao);
        for (Map.Entry<String, Value> e : msg.entrySet()) {
            String valor = e.getValue().toString();
            if ((e.getKey().equals("timestamp") || e.getKey().equals("recebido")) && e.getValue().isIntegerValue()) {
                long ms = e.getValue().asIntegerValue().toLong();
                valor = ms + " (" + FORMATO_TS.format(Instant.ofEpochMilli(ms)) + ")";
            }
            linhas.add(String.format("    %-10s: %s", e.getKey(), valor));
        }
        System.out.println(String.join("\n", linhas));
    }
}
