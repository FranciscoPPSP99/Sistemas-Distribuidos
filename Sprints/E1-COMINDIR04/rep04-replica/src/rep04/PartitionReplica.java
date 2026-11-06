package rep04;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Uso: PartitionReplica <id> <partições> [portoApi]
 *   ex.: PartitionReplica 1 0,1
 * Propriedade opcional (só afeta mensagens vindas do PubSub):
 *   -Dperda=<p>   descarta cada LOTE com probabilidade p (simula perda; gera HB_PERDIDO)
 * Consola: "stats" mostra contadores; "sair" termina.
 *
 * Uma thread de subscrição por partição + uma thread de verificação do detetor de falhas.
 * Eventos do detetor: monitor-replica<id>.txt  ->  instante;evento;p<k>;detalhe
 */
public class PartitionReplica {

    /** Estado de uma partição nesta réplica. Protegido por synchronized(particao). */
    private static final class Particao {
        final int k;
        final RecordFile file;
        final HeartbeatMonitor monitor;
        long ultimo;                                           // último seq escrito
        final TreeMap<Long, SensorRecord> retidos = new TreeMap<>();
        String host;                                           // do último LOTE recebido
        int porto = -1;
        long ultimaEscritaMs;
        int ligacoes;
        long obtidosTcp;

        Particao(int k, RecordFile file, HeartbeatMonitor monitor) {
            this.k = k;
            this.file = file;
            this.monitor = monitor;
        }
    }

    private final int id;
    private final String api;
    private final double perda;
    private final Random rnd = new Random();
    private final Map<Integer, Particao> particoes = new TreeMap<>();
    private final String monitorFile;
    private final Object logLock = new Object();

    private PartitionReplica(int id, String api, double perda) {
        this.id = id;
        this.api = api;
        this.perda = perda;
        this.monitorFile = "monitor-replica" + id + ".txt";
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Uso: PartitionReplica <id> <partições separadas por vírgulas> [portoApi]");
            return;
        }
        int id = Integer.parseInt(args[0]);
        int portoApi = args.length > 2 ? Integer.parseInt(args[2]) : 5001;
        String api = "http://127.0.0.1:" + portoApi + "/api/v0";
        double perda = Double.parseDouble(System.getProperty("perda", "0"));

        PartitionReplica r = new PartitionReplica(id, api, perda);
        for (String s : args[1].split(",")) {
            r.iniciar(Integer.parseInt(s.trim()));
        }
        r.arrancarVerificacao();
        r.consola();
    }

    private void log(String s) {
        synchronized (logLock) {
            System.out.println(s);
        }
    }

    /** Evento do detetor/recuperação: consola + monitor-replica<id>.txt com o instante à frente. */
    private void evento(String s) {
        String linha = System.currentTimeMillis() + ";" + s;
        synchronized (logLock) {
            System.out.println(linha);
            try {
                Files.writeString(Paths.get(monitorFile), linha + System.lineSeparator(),
                        StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                System.out.println("ERRO a escrever " + monitorFile + ": " + e.getMessage());
            }
        }
    }

    // ---------------------------------------------------------------- arranque

    private void iniciar(int k) throws IOException {
        HeartbeatMonitor mon = new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, this::evento);
        Particao p = new Particao(k, new RecordFile("replica-" + id + "-p" + k + ".txt"), mon);
        p.ultimo = p.file.lastSeq();
        particoes.put(k, p);
        log("Réplica " + id + " | p" + k + " | último seq no ficheiro: " + p.ultimo);

        IpfsPubSub ps = new IpfsPubSub(api);
        Thread t = new Thread(() -> {
            try {
                ps.subscribe(Wire.topicData(k), msg -> onMsg(p, msg));   // bloqueia
            } catch (Exception e) {
                log("Subscrição de p" + k + " terminou: " + e);
            }
        }, "sub-p" + k);
        t.start();
    }

    /** Thread de verificação: check() em todos os monitores a cada VERIFICACAO_MS. */
    private void arrancarVerificacao() {
        ScheduledExecutorService s = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "verificacao");
            t.setDaemon(true);
            return t;
        });
        s.scheduleAtFixedRate(() -> {
            try {
                for (Particao p : particoes.values()) {
                    p.monitor.check();
                }
            } catch (RuntimeException e) {
                log("Erro na verificação: " + e);
            }
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);
    }

    private void consola() throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        String linha;
        while ((linha = in.readLine()) != null) {
            linha = linha.trim();
            if (linha.equalsIgnoreCase("sair")) {
                System.exit(0);
            } else if (linha.equalsIgnoreCase("stats")) {
                for (Particao p : particoes.values()) {
                    synchronized (p) {
                        log("p" + p.k + ": último=" + p.ultimo + " retidos=" + p.retidos.size()
                                + " ligações=" + p.ligacoes + " obtidosTCP=" + p.obtidosTcp
                                + " líder=" + p.monitor.state()
                                + " últimaEscrita=" + p.ultimaEscritaMs + " ms");
                    }
                }
            }
        }
    }

    // ---------------------------------------------------------------- mensagens do PubSub

    private void onMsg(Particao p, String msg) {
        synchronized (p) {
            try {
                if (!"LOTE".equals(Wire.type(msg))) {
                    log("REJEITADO: " + msg.trim());
                    return;
                }
                Batch b = Batch.decode(msg, 0);   // 0: não valida a dimensão dos vetores

                // simulação de perda de um lote (só no PubSub): o detetor nem o chega a ver
                if (perda > 0 && rnd.nextDouble() < perda) {
                    log("DESCARTADO (simulação): LOTE inc=" + b.inc + " hb=" + b.hb);
                    return;
                }

                // aprende (e atualiza) o endereço do líder com cada LOTE recebido
                p.host = b.address;
                p.porto = b.port;

                // 1) o LOTE é um heartbeat (mesmo vazio)
                p.monitor.onHeartbeat(b.inc, b.hb);
                String intervalo = b.records.isEmpty() ? "-"
                        : b.records.get(0).getSeq() + ".." + b.records.get(b.records.size() - 1).getSeq();
                evento("RECEBIDO;p" + p.k + ";inc=" + b.inc + ";hb=" + b.hb + ";ultimoSeq=" + b.lastSeq
                        + ";n=" + b.records.size() + ";seq=" + intervalo);

                // 2) aplica os registos pela ordem. Um LOTE de uma incarnação antiga é ignorado
                //    pelo detetor, mas os registos NÃO se descartam: o seq é único na partição.
                for (SensorRecord r : b.records) {
                    aplicar(p, r);
                }

                // 3) faltam registos? (lacuna interna ou perda do último REG/lote anterior)
                if (p.ultimo < b.lastSeq) {
                    long de = p.ultimo + 1;
                    long ate = p.retidos.isEmpty() ? b.lastSeq : p.retidos.firstKey() - 1;
                    recuperar(p, de, ate);
                }
            } catch (UncheckedIOException e) {
                log("ERRO a escrever no ficheiro: " + e.getMessage());
            } catch (RuntimeException e) {
                log("REJEITADO: " + msg.trim() + " (" + e.getMessage() + ")");
            }
        }
    }

    // ---------------------------------------------------------------- regra de aplicação

    private void aplicar(Particao p, SensorRecord r) {
        long seq = r.getSeq();
        if (seq <= p.ultimo || p.retidos.containsKey(seq)) {
            log("DUPLICADO: " + seq);
            return;
        }
        if (seq == p.ultimo + 1) {
            escrever(p, r);
            drenar(p);
        } else {
            p.retidos.put(seq, r);
            log("EM ESPERA: " + seq + " (esperava " + (p.ultimo + 1) + ", retidos=" + p.retidos.size() + ")");
        }
    }

    private void escrever(Particao p, SensorRecord r) {
        try {
            p.file.append(r);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        p.ultimo = r.getSeq();
        p.ultimaEscritaMs = System.currentTimeMillis();
    }

    /** Escreve os retidos que passaram a estar em sequência. */
    private void drenar(Particao p) {
        while (!p.retidos.isEmpty()) {
            long primeiro = p.retidos.firstKey();
            if (primeiro <= p.ultimo) {
                p.retidos.pollFirstEntry();                    // já coberto
            } else if (primeiro == p.ultimo + 1) {
                escrever(p, p.retidos.pollFirstEntry().getValue());
            } else {
                break;
            }
        }
    }

    // ---------------------------------------------------------------- recuperação (TCP)

    private void recuperar(Particao p, long de, long ate) {
        if (de > ate || p.host == null) {
            return;
        }
        evento("RECUPERACAO;p" + p.k + ";de=" + de + ";ate=" + ate);
        p.ligacoes++;

        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(p.host, p.porto), Wire.T_TCP_MS);   // bloqueia até ser aceite
            s.setSoTimeout(Wire.T_TCP_MS);                                      // readLine com timeout
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
                SensorRecord r = SensorRecord.fromLine(linha);
                p.obtidosTcp++;
                log("RECUPERADO: " + r.getSeq());
                aplicar(p, r);                                  // mesma regra de aplicação
            }
            if (!fim) {
                throw new IOException("resposta interrompida antes de FIM");
            }
        } catch (IOException | RuntimeException e) {
            // o que já foi escrito mantém-se; a condição ultimo < ultimoSeq continua
            // verdadeira, por isso volta a tentar-se no próximo LOTE desta partição
            log("RECUPERAÇÃO FALHOU: " + e.getMessage());
        }
        drenar(p);
    }
}
