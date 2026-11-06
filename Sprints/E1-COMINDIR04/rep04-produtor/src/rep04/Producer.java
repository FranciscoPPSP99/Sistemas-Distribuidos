package rep04;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Uso: Producer <N> [api]
 * Comandos:
 *   <sensor> <temperatura>
 *   rajada <n> <s>
 *   rajada-quente <n> <s>
 *   sair
 *
 * Subscreve os tópicos LOTE de todas as partições (só o cabeçalho) e mantém um
 * HeartbeatMonitor por partição; eventos em monitor-produtor.txt.
 * Assume que IpfsPubSub tem publish(topico, msg) e subscribe(topico, handler).
 */
public class Producer {

    private static final Random RND = new Random();
    private static final String MONITOR_FILE = "monitor-produtor.txt";
    private static final Object LOG_LOCK = new Object();
    private static HeartbeatMonitor[] monitores;

    /** Evento do detetor: consola + monitor-produtor.txt, com o instante à frente. */
    private static void evento(String s) {
        String linha = System.currentTimeMillis() + ";" + s;
        synchronized (LOG_LOCK) {
            System.out.println(linha);
            try {
                Files.writeString(Paths.get(MONITOR_FILE), linha + System.lineSeparator(),
                        StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                System.out.println("ERRO a escrever " + MONITOR_FILE + ": " + e.getMessage());
            }
        }
    }

    private static void arrancarDetetor(IpfsPubSub ps, int n) {
        monitores = new HeartbeatMonitor[n];
        for (int k = 0; k < n; k++) {
            monitores[k] = new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, Producer::evento);
        }
        for (int k = 0; k < n; k++) {
            final int part = k;
            Thread t = new Thread(() -> {
                try {
                    ps.subscribe(Wire.topicData(part), msg -> {
                        try {
                            if (!"LOTE".equals(Wire.type(msg))) return;
                            Batch b = Batch.decodeHeader(msg);   // só o cabeçalho interessa
                            monitores[part].onHeartbeat(b.inc, b.hb);
                            // n vem do cabeçalho (último campo da 1.ª linha)
                            String[] cab = msg.strip().split("\\R", 2)[0].split(";", -1);
                            evento("RECEBIDO;p" + part + ";inc=" + b.inc + ";hb=" + b.hb
                                    + ";ultimoSeq=" + b.lastSeq + ";n=" + cab[6].trim());
                        } catch (RuntimeException e) {
                            System.out.println("LOTE rejeitado em p" + part + ": " + e.getMessage());
                        }
                    });
                } catch (Exception e) {
                    System.out.println("Subscrição de p" + part + " terminou: " + e);
                }
            }, "prod-sub-p" + k);
            t.setDaemon(true);
            t.start();
        }
        ScheduledExecutorService s = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "verificacao");
            t.setDaemon(true);
            return t;
        });
        s.scheduleAtFixedRate(() -> {
            try {
                for (HeartbeatMonitor m : monitores) m.check();
            } catch (RuntimeException e) {
                System.out.println("Erro na verificação: " + e);
            }
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Uso: Producer <N> [api]");
            return;
        }
        int n = Integer.parseInt(args[0]);
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";

        Partitioner part = new Partitioner(n);
        IpfsPubSub ps = new IpfsPubSub(api);
        arrancarDetetor(ps, n);

        System.out.println("Produtor pronto (N=" + n + "). Comandos: <sensor> <temp> | rajada <n> <s> | rajada-quente <n> <s> | sair");
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        String linha;
        while ((linha = in.readLine()) != null) {
            linha = linha.trim();
            if (linha.isEmpty()) {
                continue;
            }
            if (linha.equalsIgnoreCase("sair")) {
                break;
            }
            String[] t = linha.split("\\s+");
            try {
                if (t[0].equals("rajada") && t.length == 3) {
                    rajada(ps, part, Integer.parseInt(t[1]), Integer.parseInt(t[2]), false);
                } else if (t[0].equals("rajada-quente") && t.length == 3) {
                    rajada(ps, part, Integer.parseInt(t[1]), Integer.parseInt(t[2]), true);
                } else if (t.length == 2) {
                    double temp = Double.parseDouble(t[1].replace(',', '.'));
                    enviar(ps, part, t[0], temp);
                } else {
                    System.out.println("Comando inválido: " + linha);
                }
            } catch (NumberFormatException e) {
                System.out.println("Número inválido: " + linha);
            } catch (Exception e) {
                System.out.println("Falha a publicar: " + e);
            }
        }
    }

    private static int enviar(IpfsPubSub ps, Partitioner part, String sensor, double temp) throws Exception {
        int k = part.of(sensor);
        if (monitores != null && monitores[k].state() == HeartbeatMonitor.State.SUSPEITO) {
            System.out.println("AVISO: líder p" + k + " suspeito; leitura " + sensor + " pode perder-se");
        }
        // O produtor inicia cada evento sem conhecimento de eventos anteriores.
        ps.publish(Wire.topicIn(k), Wire.in(sensor, temp, 0L, VectorClock.zeros(part.count())));
        return k;
    }

    private static void rajada(IpfsPubSub ps, Partitioner part, int total, int s, boolean quente) throws Exception {
        int[] porParticao = new int[part.count()];
        long primeira = 0;
        long ultima = 0;
        for (int i = 0; i < total; i++) {
            String sensor = escolherSensor(s, quente);
            double temp = Math.round((15 + RND.nextDouble() * 15) * 10) / 10.0;
            long agora = System.currentTimeMillis();
            if (i == 0) {
                primeira = agora;
            }
            int k = enviar(ps, part, sensor, temp);
            ultima = System.currentTimeMillis();
            porParticao[k]++;
        }
        System.out.println("Rajada terminada: " + total + " leituras");
        System.out.println("  primeira publicação: " + primeira + " ms; última: " + ultima + " ms; duração: " + (ultima - primeira) + " ms");
        for (int k = 0; k < porParticao.length; k++) {
            System.out.println("  p" + k + ": " + porParticao[k] + " leituras");
        }
    }

    private static String escolherSensor(int s, boolean quente) {
        if (quente) {
            if (s < 2 || RND.nextDouble() < 0.8) {
                return "S01";
            }
            return String.format("S%02d", 2 + RND.nextInt(s - 1));   // S02..S<s>
        }
        return String.format("S%02d", 1 + RND.nextInt(s));           // S01..S<s>
    }
}
