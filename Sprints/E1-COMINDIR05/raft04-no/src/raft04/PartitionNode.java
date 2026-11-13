package raft04;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
/**
 * Uso: PartitionNode <k> <i> [N]
 *
 * Nó i (1..GRUPO) do grupo da partição de dados k. Junta o PartitionLeader e a
 * Replica:
 * qualquer nó pode ser seguidor, candidato ou líder (papel gerido por
 * RaftState).
 *
 * Atividades concorrentes:
 * - subscrição de raft04-in-p<k> -> tratarIn (só o líder atribui seq)
 * - subscrição de raft04-p<k> -> tratarLote (seguidor: aplica lotes)
 * - subscrição de raft04-eleicao-p<k> -> tratarEleicao (PEDIDO_VOTO e VOTO)
 * - verificação (VERIFICACAO_MS) -> tick() do RaftState; publica PEDIDO_VOTO
 * - lote/heartbeat (PERIODO_HB_MS) -> enviarLote(), ativo só enquanto este nó é
 * líder
 * - recuperação: thread principal, ServerSocket (serve registos a qualquer nó)
 *
 * Estado partilhado do nó (ultimoSeq, ultimoTermo, L, V, pendentes, hb,
 * retidos): protegido por 'lock'.
 * Ordem de aquisição: 'lock' do nó -> monitor do RaftState -> logLock. Nunca no
 * sentido inverso.
 * O servidor TCP NÃO adquire 'lock' (lê só variáveis voláteis), para dois nós
 * que se recuperam
 * um ao outro em simultâneo não ficarem à espera um do outro.
 */
public class PartitionNode {
    private final int k;
    private final int i;
    private final int n;
    private final String host = "127.0.0.1";
    private final int porto;
    private final Partitioner partitioner;
    private final IpfsPubSub ps;
    private final String api = "http://127.0.0.1:5001/api/v0";
    private final Path ficheiro;
    private final RecordFile file;
    private final String hbFile;
    private final String eventosFile;
    private final RaftState raft;
    private final Object lock = new Object();
    private final Object logLock = new Object();
    private final ScheduledExecutorService sched;
    private ScheduledFuture<?> agendamento; // enviarLote(); só existe enquanto é líder
    // último registo local (líder e seguidor): lido sem 'lock' pelo servidor TCP e
    // pelo tick
    private volatile long ultimoSeq;
    private volatile long ultimoTermo;
    // estado do líder (inicializado em iniciarLider)
    private long lamport;
    private VectorClock vector;
    private List<SensorRecord> pendentes = new ArrayList<>();
    private long hb = 0;
    // estado do seguidor
    private final TreeMap<Long, SensorRecord> retidos = new TreeMap<>();
    private long termoRetidos = -1;
    private PartitionNode(int k, int i, int n) throws IOException {
        this.k = k;
        this.i = i;
        this.n = n;
        this.sched = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "sched-p" + k + "-n" + i);
            t.setDaemon(true);
            return t;
        });
        this.porto = Wire.nodePort(k, i);
        this.partitioner = new Partitioner(n);
        this.ps = new IpfsPubSub(api);
        this.ficheiro = Paths.get("no-p" + k + "-n" + i + ".txt");
        this.file = new RecordFile(ficheiro.toString());
        this.hbFile = "hb-p" + k + "-n" + i + ".txt";
        this.eventosFile = "eventos-p" + k + "-n" + i + ".txt";
        SensorRecord last = ultimoRegisto();
        this.ultimoSeq = last == null ? 0 : last.getSeq();
        this.ultimoTermo = last == null ? 0 : last.getTerm();
        this.raft = new RaftState(k, i, Wire.GRUPO, Wire.ELEICAO_MIN_MS, Wire.ELEICAO_MAX_MS,
                Paths.get("estado-p" + k + "-n" + i + ".txt"), this::evento);
    }
    /** Último registo guardado no ficheiro do nó (null se ainda não existe). */
    private SensorRecord ultimoRegisto() throws IOException {
        return file.last();
    }
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Uso: PartitionNode <k> <i> [N]");
            return;
        }
        int k = Integer.parseInt(args[0]);
        int i = Integer.parseInt(args[1]);
        int n = args.length > 2 ? Integer.parseInt(args[2]) : 3;
        if (i < 1 || i > Wire.GRUPO) {
            throw new IllegalArgumentException("i tem de estar em 1.." + Wire.GRUPO);
        }
        new PartitionNode(k, i, n).run();
    }
    private void run() throws IOException {
        System.out.println("Nó n" + i + " de p" + k + " | porto " + porto + " | termo=" + raft.term()
                + " | último seq=" + ultimoSeq + " (termo " + ultimoTermo + ")"
                + " | período=" + Wire.PERIODO_HB_MS + " ms | eleição=[" + Wire.ELEICAO_MIN_MS
                + ", " + Wire.ELEICAO_MAX_MS + "] ms");
        // 1) abrir primeiro o porto: se estiver ocupado, falha logo aqui
        // (BindException)
        try (ServerSocket server = new ServerSocket(porto)) {
            // instante de arranque, para o RaftCheck medir o tempo até ao primeiro líder
            evento("ARRANQUE;p" + k + ";termo=" + raft.term() + ";ultimoSeq=" + ultimoSeq
                    + ";ultimoTermo=" + ultimoTermo);
            // 2) subscrições (cada uma bloqueia na sua thread)
            subscrever(Wire.topicIn(k), this::tratarIn, "in");
            subscrever(Wire.topicData(k), this::tratarLote, "dados");
            subscrever(Wire.topicElection(k), this::tratarEleicao, "eleicao");
            // 3) verificação do prazo de eleição
            sched.scheduleAtFixedRate(this::verificar, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS,
                    TimeUnit.MILLISECONDS);
            // 4) recuperação: uma ligação de cada vez, a qualquer nó
            while (true) {
                try (Socket s = server.accept();
                        BufferedReader in = new BufferedReader(
                                new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                        PrintWriter out = new PrintWriter(
                                new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true)) {
                    s.setSoTimeout(Wire.T_TCP_MS);
                    atender(s, in, out);
                } catch (IOException e) {
                    log("Recuperação interrompida: " + e.getMessage());
                }
            }
        }
    }
    // ---------------------------------------------------------------- registo de
    // eventos
    private void log(String s) {
        synchronized (logLock) {
            System.out.println(s);
        }
    }
    /** Evento do nó: consola + eventos-p<k>-n<i>.txt, com o instante à frente. */
    private void evento(String s) {
        String linha = System.currentTimeMillis() + ";" + s;
        synchronized (logLock) {
            System.out.println(linha);
            try {
                Files.writeString(Paths.get(eventosFile), linha + System.lineSeparator(),
                        StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                System.out.println("ERRO a escrever " + eventosFile + ": " + e.getMessage());
            }
        }
    }
    // ---------------------------------------------------------------- subscrições
    // e eleição
    private void subscrever(String topico, Consumer<String> tratar, String nome) {
        IpfsPubSub sub = new IpfsPubSub(api);
        Thread t = new Thread(() -> {
            try {
                sub.subscribe(topico, tratar); // bloqueia enquanto a subscrição durar
            } catch (Exception e) {
                evento("ERRO;p" + k + ";subscrição " + nome + " terminou: " + e);
                System.exit(1);
            }
        }, "sub-" + nome + "-p" + k + "-n" + i);
        t.setDaemon(true);
        t.start();
    }
    /**
     * Thread de verificação: se o prazo expirou, tick() grava o novo estado e
     * devolve o termo.
     */
    private void verificar() {
        try {
            long termo = raft.tick();
            if (termo >= 0) {
                // o estado persistente já foi gravado por tick() ANTES de publicar
                publicar(Wire.topicElection(k),
                        new VoteRequest(termo, i, ultimoSeq, ultimoTermo).encode());
            }
        } catch (Exception e) {
            // nunca deixar a excepção terminar o scheduleAtFixedRate
            evento("ERRO;p" + k + ";verificação: " + e);
        }
    }
    private void tratarEleicao(String msg) {
        try {
            if (Wire.isolated(k, i)) {
                return; // partição de rede simulada
            }
            String tipo = Wire.type(msg);
            if (tipo.equals("PEDIDO_VOTO")) {
                VoteRequest req = VoteRequest.decode(msg);
                if (req.candidate == i) {
                    return; // o meu próprio pedido
                }
                RaftState.Vote v;
                synchronized (lock) {
                    v = raft.onVoteRequest(req.term, req.candidate, req.lastSeq, req.lastTerm,
                            ultimoSeq, ultimoTermo);
                }
                // responde sempre, mesmo quando recusa; o estado já foi gravado por
                // onVoteRequest
                publicar(Wire.topicElection(k), new VoteReply(v.term, i, req.candidate, v.granted).encode());
            } else if (tipo.equals("VOTO")) {
                VoteReply rep = VoteReply.decode(msg);
                if (rep.candidate != i || rep.voter == i) {
                    return; // não é para mim
                }
                synchronized (lock) {
                    if (raft.onVote(rep.term, rep.voter, rep.candidate, rep.granted)) {
                        iniciarLider(); // mesma secção: IN não vê líder por inicializar
                    }
                }
            } else {
                evento("REJEITADO;p" + k + ";" + msg.trim());
            }
        } catch (IOException e) {
            evento("ERRO;p" + k + ";eleição: " + e.getMessage());
        } catch (RuntimeException e) {
            evento("REJEITADO;p" + k + ";" + msg.trim() + " (" + e.getMessage() + ")");
        }
    }
    private void publicar(String topico, String msg) {
        if (Wire.isolated(k, i)) {
            return;
        }
        try {
            ps.publish(topico, msg);
        } catch (Exception e) {
            evento("ERRO;p" + k + ";a publicar em " + topico + ": " + e);
        }
    }
    // ---------------------------------------------------------------- papel de
    // líder
    /** Chamada com 'lock' adquirido, no instante em que onVote devolve true. */
    private void iniciarLider() throws IOException {
        SensorRecord last = file.last();
        if (last == null) {
            lamport = 0;
            vector = VectorClock.zeros(n);
        } else {
            lamport = last.getLamport();
            vector = last.getVector();
            if (vector.size() != n) {
                throw new IOException("O vetor guardado tem " + vector.size()
                        + " componentes, mas N=" + n + "; use ficheiros novos ou a configuracao original.");
            }
        }
        hb = 0;
        pendentes = new ArrayList<>();
        if (agendamento != null) {
            agendamento.cancel(false);
        }
        agendamento = sched.scheduleAtFixedRate(this::enviarLote, 0, Wire.PERIODO_HB_MS,
                TimeUnit.MILLISECONDS);
    }
    private void enviarLote() {
        try {
            if (Wire.isolated(k, i)) {
                return; // isolado: não publica, mas continua líder
            }
            long termo;
            List<SensorRecord> lote;
            long ultimo;
            long ultTermo;
            long numHb;
            // MESMA secção sincronizada que a atribuição de seq: fotografia consistente.
            synchronized (lock) {
                termo = raft.termIfLeader();
                if (termo < 0) {
                    if (agendamento != null) {
                        agendamento.cancel(false); // já não sou líder
                    }
                    return;
                }
                lote = pendentes;
                pendentes = new ArrayList<>();
                ultimo = ultimoSeq;
                ultTermo = ultimoTermo;
                numHb = ++hb;
            }
            // FORA da secção sincronizada: a publicação no IPFS bloqueia (I/O).
            String msg = new Batch(termo, i, host, porto, numHb, ultimo, ultTermo, lote).encode();
            int bytes = msg.getBytes(StandardCharsets.UTF_8).length;
            try {
                ps.publish(Wire.topicData(k), msg);
            } catch (Exception e) {
                // os seguidores recuperam os registos através do ultimoSeq do lote seguinte
                evento("ERRO;p" + k + ";a publicar lote hb=" + numHb + ": " + e);
                return;
            }
            Files.writeString(Paths.get(hbFile),
                    termo + ";" + numHb + ";" + System.currentTimeMillis() + ";" + ultimo + ";"
                            + ultTermo + ";" + lote.size() + ";" + bytes + System.lineSeparator(),
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            log("HEARTBEAT: termo=" + termo + " hb=" + numHb + " n=" + lote.size()
                    + " ultimoSeq=" + ultimo);
        } catch (Exception e) {
            // nunca deixar a excepção terminar o scheduleAtFixedRate
            evento("ERRO;p" + k + ";enviarLote: " + e);
        }
    }
    private void tratarIn(String msg) {
        try {
            if (!"IN".equals(Wire.type(msg))) {
                throw new IllegalArgumentException("tipo inesperado");
            }
            String[] f = Wire.body(msg).split(";", -1);
            if (f.length != 4 || f[0].isBlank()) {
                throw new IllegalArgumentException("IN mal formado: esperava sensor;temperatura;L;V");
            }
            String sensor = f[0].trim();
            double temp = Double.parseDouble(f[1].trim());
            long incomingLamport = Long.parseLong(f[2].trim());
            VectorClock incomingVector = VectorClock.parse(f[3]);
            if (incomingLamport < 0 || incomingVector.size() != partitioner.count()) {
                throw new IllegalArgumentException("carimbos invalidos ou vetor com dimensao errada");
            }
            int j = partitioner.of(sensor);
            if (j != k) {
                log("PARTIÇÃO ERRADA: " + sensor + " pertence a p" + j);
                return;
            }
            SensorRecord r;
            synchronized (lock) {
                long termo = raft.termIfLeader();
                if (termo < 0) {
                    evento("IGNORADO;p" + k + ";IN sensor=" + sensor + " (não sou líder)");
                    return;
                }
                // Calcular em copias: so alterar o estado depois de gravar com sucesso.
                long nextSeq = ultimoSeq + 1;
                long nextLamport = Math.max(lamport, incomingLamport) + 1;
                VectorClock nextVector = vector.copy();
                nextVector.mergeMax(incomingVector);
                nextVector.increment(k);
                if (nextVector.get(k) != nextSeq) {
                    throw new IllegalStateException("Invariante causal violada: V[" + k + "]="
                            + nextVector.get(k) + ", mas seq=" + nextSeq
                            + ". Verifique os carimbos recebidos e o estado persistido.");
                }
                r = new SensorRecord(nextSeq, termo, sensor, temp,
                        Long.toString(System.currentTimeMillis()), nextLamport, nextVector);
                file.append(r); // persistir antes de confirmar o estado
                ultimoSeq = nextSeq;
                ultimoTermo = termo;
                lamport = nextLamport;
                vector = nextVector;
                pendentes.add(r); // fica à espera do próximo heartbeat
            }
            log("EVENTO: p" + k + " seq=" + r.getSeq() + " termo=" + r.getTerm() + " sensor="
                    + r.getSensorId() + " temperatura=" + r.getTemperature() + " L=" + r.getLamport()
                    + " V=" + r.getVector() + " (pendente)");
        } catch (IllegalArgumentException e) {
            log("REJEITADO: " + msg.trim() + " (" + e.getMessage() + ")");
        } catch (Exception e) {
            evento("ERRO;p" + k + ";a processar leitura: " + e);
        }
    }
    // ---------------------------------------------------------------- papel de
    // seguidor
    private void tratarLote(String msg) {
        try {
            if (Wire.isolated(k, i)) {
                return; // partição de rede simulada
            }
            if (!"LOTE".equals(Wire.type(msg))) {
                log("REJEITADO: " + msg.trim());
                return;
            }
            Batch b = Batch.decode(msg, n);
            if (b.leader == i) {
                return; // o meu próprio lote
            }
            synchronized (lock) {
                processarLote(b);
            }
        } catch (IOException e) {
            evento("ERRO;p" + k + ";a escrever no ficheiro: " + e.getMessage());
        } catch (RuntimeException e) {
            log("REJEITADO: " + msg.trim() + " (" + e.getMessage() + ")");
        }
    }
    /** Chamada com 'lock' adquirido. */
    private void processarLote(Batch b) throws IOException {
        // onBatch adota o termo (e demite este nó, se for o caso) ou rejeita o líder
        // obsoleto
        if (!raft.onBatch(b.term, b.leader)) {
            evento("IGNORADO;p" + k + ";LOTE termo=" + b.term + ";lider=n" + b.leader
                    + ";termoAtual=" + raft.term());
            return;
        }
        String intervalo = b.records.isEmpty() ? "-"
                : b.records.get(0).getSeq() + ".." + b.records.get(b.records.size() - 1).getSeq();
        evento("RECEBIDO;p" + k + ";termo=" + b.term + ";lider=n" + b.leader + ";hb=" + b.hb
                + ";ultimoSeq=" + b.lastSeq + ";ultimoTermo=" + b.lastTerm
                + ";n=" + b.records.size() + ";seq=" + intervalo);
        // registos retidos de outro termo já não servem
        if (b.term != termoRetidos) {
            retidos.clear();
            termoRetidos = b.term;
        }
        // 1) aplica os registos pela ordem
        boolean diverge = false;
        for (SensorRecord r : b.records) {
            if (!aplicar(r)) {
                diverge = true;
                break;
            }
        }
        // 2) faltam registos? (lacuna interna ou perda do último lote anterior)
        if (!diverge && ultimoSeq < b.lastSeq) {
            long de = ultimoSeq + 1;
            long ate = retidos.isEmpty() ? b.lastSeq : retidos.firstKey() - 1;
            diverge = recuperar(b, de, ate);
        }
        // 3) divergência: registos a mais, ou último registo com termo diferente do do
        // líder
        if (!diverge) {
            diverge = ultimoSeq > b.lastSeq
                    || (ultimoSeq == b.lastSeq && ultimoSeq > 0 && ultimoTermo != b.lastTerm);
        }
        if (diverge) {
            resolverDivergencia(b);
        }
    }
    /**
     * Devolve false se o registo revela uma divergência (mesmo seq, termo
     * diferente).
     */
    private boolean aplicar(SensorRecord r) throws IOException {
        long seq = r.getSeq();
        if (seq <= ultimoSeq) {
            SensorRecord l = local(seq);
            if (l != null && l.getTerm() == r.getTerm()) {
                log("DUPLICADO: " + seq);
                return true;
            }
            return false; // mesmo seq, outro termo
        }
        if (retidos.containsKey(seq)) {
            log("DUPLICADO: " + seq);
            return true;
        }
        if (seq == ultimoSeq + 1) {
            escrever(r);
            drenar();
        } else {
            retidos.put(seq, r);
            log("EM ESPERA: " + seq + " (esperava " + (ultimoSeq + 1) + ", retidos=" + retidos.size() + ")");
        }
        return true;
    }
    private void escrever(SensorRecord r) throws IOException {
        file.append(r);
        ultimoSeq = r.getSeq();
        ultimoTermo = r.getTerm();
    }
    /** Escreve os retidos que passaram a estar em sequência. */
    private void drenar() throws IOException {
        while (!retidos.isEmpty()) {
            long primeiro = retidos.firstKey();
            if (primeiro <= ultimoSeq) {
                retidos.pollFirstEntry(); // já coberto
            } else if (primeiro == ultimoSeq + 1) {
                escrever(retidos.pollFirstEntry().getValue());
            } else {
                break;
            }
        }
    }
    private SensorRecord local(long seq) throws IOException {
        List<SensorRecord> l = file.range(seq, seq);
        return l.isEmpty() ? null : l.get(0);
    }
    // ---------------------------------------------------------------- recuperação
    // (cliente TCP)
    /**
     * Pede ao líder [de, ate]. Devolve null se a ligação falhar (tenta-se de novo
     * no próximo LOTE).
     */
    private List<SensorRecord> pedir(Batch b, long de, long ate) {
        try {
            if (Wire.isolated(k, i)) {
                throw new IOException("nó isolado");
            }
            List<SensorRecord> recs = new ArrayList<>();
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(b.address, b.port), Wire.T_TCP_MS);
                s.setSoTimeout(Wire.T_TCP_MS);
                PrintWriter out = new PrintWriter(
                        new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true);
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                out.println("PEDIDO " + de + " " + ate);
                boolean fim = false;
                String linha;
                while ((linha = in.readLine()) != null) {
                    if (linha.equals("FIM")) {
                        fim = true;
                        break;
                    }
                    if (linha.startsWith("ERRO")) {
                        throw new IOException("o líder respondeu: " + linha);
                    }
                    recs.add(SensorRecord.fromLine(linha));
                }
                if (!fim) {
                    throw new IOException("resposta interrompida antes de FIM");
                }
            }
            return recs;
        } catch (IOException | RuntimeException e) {
            log("RECUPERAÇÃO FALHOU: " + e.getMessage());
            return null;
        }
    }
    /**
     * Recupera registos em falta. Devolve true se algum revelou uma divergência.
     */
    private boolean recuperar(Batch b, long de, long ate) throws IOException {
        if (de > ate) {
            return false;
        }
        evento("RECUPERACAO;p" + k + ";de=" + de + ";ate=" + ate);
        List<SensorRecord> recs = pedir(b, de, ate);
        if (recs == null) {
            return false; // o que já foi escrito mantém-se
        }
        for (SensorRecord r : recs) {
            log("RECUPERADO: " + r.getSeq());
            if (!aplicar(r)) { // mesma regra de aplicação
                return true;
            }
        }
        drenar();
        return false;
    }
    // ---------------------------------------------------------------- divergência
    // e truncagem

    /**
     * 
     * Pede ao líder os registos a partir do primeiro seq do último bloco de termo,
     * recua um bloco
     * 
     * de cada vez enquanto o primeiro registo recebido não coincidir com o local,
     * trunca o ficheiro
     * 
     * a partir do primeiro seq diferente e aplica os registos do líder.
     * 
     */
    private void resolverDivergencia(Batch b) throws IOException {
        if (ultimoSeq == 0) {
            return;
        }
        long de = inicioBlocoTermo(ultimoSeq);
        List<SensorRecord> recs;
        while (true) {
            recs = pedir(b, de, b.lastSeq);
            if (recs == null) {
                return;
            }
            SensorRecord l = local(de);
            boolean coincide = !recs.isEmpty() && recs.get(0).getSeq() == de
                    && l != null && l.getTerm() == recs.get(0).getTerm();
            if (coincide || de <= 1) {
                break;
            }
            de = inicioBlocoTermo(de - 1);
        }

        // primeiro seq em que o líder e este nó diferem
        long diff = -1;
        for (SensorRecord r : recs) {
            SensorRecord l = local(r.getSeq());
            if (l == null || l.getTerm() != r.getTerm()) {
                diff = r.getSeq();
                break;
            }
        }
        if (diff < 0) {
            diff = recs.isEmpty() ? de : recs.get(recs.size() - 1).getSeq() + 1;
        }

        if (diff <= ultimoSeq) {
            List<SensorRecord> descartados = file.range(diff, ultimoSeq);
            registarTruncados(diff, descartados); // para o RaftCheck
            truncar(diff);
            String termos = new TreeSet<>(descartados.stream().map(SensorRecord::getTerm)
                    .collect(Collectors.toList())).stream().map(String::valueOf)
                    .collect(Collectors.joining(","));
            evento("TRUNCADO;p" + k + ";de=" + diff + ";descartados=" + descartados.size()
                    + ";termos=" + termos);
        }
        retidos.clear();
        for (SensorRecord r : recs) {
            if (r.getSeq() >= diff) {
                aplicar(r);
            }
        }
    }

    /**
     * Guarda os registos descartados em truncados-p<k>-n<i>.txt (bloco
     * '#TRUNCADO;de=..;n=..' + linhas).
     */
    private void registarTruncados(long de, List<SensorRecord> descartados) throws IOException {
        StringBuilder sb = new StringBuilder("#TRUNCADO;de=" + de + ";n=" + descartados.size())
                .append(System.lineSeparator());
        for (SensorRecord r : descartados) {
            sb.append(r.toLine()).append(System.lineSeparator());
        }
        Files.writeString(Paths.get("truncados-p" + k + "-n" + i + ".txt"), sb.toString(),
                StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    /**
     * Primeiro seq do bloco final de registos com o mesmo termo, até ao registo
     * 'ate'.
     */
    private long inicioBlocoTermo(long ate) throws IOException {
        List<SensorRecord> l = file.range(1, ate);
        int idx = l.size() - 1;
        long termo = l.get(idx).getTerm();
        while (idx > 0 && l.get(idx - 1).getTerm() == termo) {
            idx--;
        }
        return l.get(idx).getSeq();
    }

    /** Reescreve o ficheiro de forma atómica, sem os registos com seq >= de. */
    private void truncar(long de) throws IOException {
        List<SensorRecord> manter = file.range(1, de - 1);
        Path tmp = ficheiro.resolveSibling(ficheiro.getFileName() + ".tmp");
        Files.write(tmp, manter.stream().map(SensorRecord::toLine).collect(Collectors.toList()),
                StandardCharsets.UTF_8);
        Files.move(tmp, ficheiro, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        ultimoSeq = manter.isEmpty() ? 0 : manter.get(manter.size() - 1).getSeq();
        ultimoTermo = manter.isEmpty() ? 0 : manter.get(manter.size() - 1).getTerm();
    }

    // ---------------------------------------------------------------- recuperação
    // (servidor TCP)

    /**
     * Serve registos do ficheiro deste nó, seja ou não líder. Não adquire 'lock'.
     */
    private void atender(Socket s, BufferedReader in, PrintWriter out) throws IOException {
        if (Wire.isolated(k, i)) {
            return; // as ligações de um nó isolado falham
        }
        String pedido = in.readLine(); // bloqueia até chegar o pedido (ou timeout)
        if (pedido == null) {
            return;
        }
        String[] t = pedido.trim().split("\\s+");
        long de;
        long ate;
        try {
            if (t.length != 3 || !t[0].equals("PEDIDO")) {
                throw new NumberFormatException();
            }
            de = Long.parseLong(t[1]);
            ate = Long.parseLong(t[2]);
        } catch (NumberFormatException e) {
            out.println("ERRO pedido mal formado");
            return;
        }
        if (de < 1 || de > ate) {
            out.println("ERRO intervalo inválido");
            return;
        }
        ate = Math.min(ate, ultimoSeq);
        List<SensorRecord> regs = file.range(de, ate); // o ficheiro do nó é a fonte
        for (SensorRecord r : regs) {
            out.println(r.toLine());
        }
        out.println("FIM");
        log("RECUPERAÇÃO servida: " + de + ".." + ate + " para " + s.getRemoteSocketAddress()
                + " (" + regs.size() + " registos)");
    }
}
