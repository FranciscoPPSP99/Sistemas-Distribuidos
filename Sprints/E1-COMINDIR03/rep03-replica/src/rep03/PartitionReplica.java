package rep03;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

/**
 * Uso: PartitionReplica <id> <partições> [portoApi]
 *   ex.: PartitionReplica 1 0,1
 * Propriedades opcionais (só afetam mensagens vindas do PubSub):
 *   -Dperda=<p>        descarta cada REG com probabilidade p
 *   -Ddescartar=<seq>  descarta só a primeira receção desse seq
 * Consola: "stats" mostra contadores; "sair" termina.
 *
 * Uma thread de subscrição por partição, cada uma com o seu estado (classe Particao).
 * A recuperação TCP corre na própria thread de subscrição da partição.
 *
 * Assume que IpfsPubSub tem subscribe(String topico, Consumer<String> handler) (bloqueante)
 * e que SensorRecord/RecordFile têm fromLine, toLine, getSeq, append, lastSeq.
 */
public class PartitionReplica {

    /** Estado de uma partição nesta réplica. Protegido por synchronized(particao). */
    private static final class Particao {
        final int k;
        final RecordFile file;
        long ultimo;                                           // último seq escrito
        final TreeMap<Long, SensorRecord> retidos = new TreeMap<>();
        String host;                                           // do último REG recebido
        int porto = -1;
        boolean descartouAlvo;
        long ultimaEscritaMs;
        int ligacoes;
        long obtidosTcp;

        Particao(int k, RecordFile file) {
            this.k = k;
            this.file = file;
        }
    }

    private final int id;
    private final String api;
    private final double perda;
    private final long descartar;
    private final Random rnd = new Random();
    private final Map<Integer, Particao> particoes = new TreeMap<>();

    private PartitionReplica(int id, String api, double perda, long descartar) {
        this.id = id;
        this.api = api;
        this.perda = perda;
        this.descartar = descartar;
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
        long descartar = Long.parseLong(System.getProperty("descartar", "-1"));

        PartitionReplica r = new PartitionReplica(id, api, perda, descartar);
        for (String s : args[1].split(",")) {
            r.iniciar(Integer.parseInt(s.trim()));
        }
        r.consola();
    }

    private synchronized static void log(String s) {
        System.out.println(s);
    }

    // ---------------------------------------------------------------- arranque

    private void iniciar(int k) throws IOException {
        Particao p = new Particao(k, new RecordFile("replica-" + id + "-p" + k + ".txt"));
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
                if (!"REG".equals(Wire.type(msg))) {
                    log("REJEITADO: " + msg.trim());
                    return;
                }
                String[] partes = Wire.regParts(msg);
                String host = partes[0];
                int porto = Integer.parseInt(partes[1].trim());
                SensorRecord r = SensorRecord.fromLine(partes[2]);
                long seq = r.getSeq();

                // simulação de perda (só no PubSub)
                if (descartar == seq && !p.descartouAlvo) {
                    p.descartouAlvo = true;
                    log("DESCARTADO (simulação): " + seq);
                    return;
                }
                if (perda > 0 && rnd.nextDouble() < perda) {
                    log("DESCARTADO (simulação): " + seq);
                    return;
                }

                // aprende (e atualiza) o endereço do líder com cada REG recebido
                p.host = host;
                p.porto = porto;

                aplicar(p, r);
                if (p.retidos.size() > Wire.LIMITE_RETIDOS) {
                    recuperar(p);
                }
            } catch (UncheckedIOException e) {
                log("ERRO a escrever no ficheiro: " + e.getMessage());
            } catch (RuntimeException e) {
                log("REJEITADO: " + msg.trim());
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

    private void recuperar(Particao p) {
        long de = p.ultimo + 1;
        long ate = p.retidos.firstKey() - 1;
        log("RECUPERAÇÃO: " + de + ".." + ate + " pedida a " + p.host + ":" + p.porto);
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
            // o que já foi escrito mantém-se; os retidos ficam; a condição continua
            // verdadeira, por isso volta a tentar-se no próximo REG desta partição
            log("RECUPERAÇÃO FALHOU: " + e.getMessage());
        }
        drenar(p);
    }
}
