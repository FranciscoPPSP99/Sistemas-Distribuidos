package rep04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Subscreve os tópicos LOTE de todas as partições. Mantém o conhecimento causal acumulado
 * que observou e publica ALARM-<sensor> quando recebe uma leitura >= LIMIAR_ALARME.
 * Mantém um HeartbeatMonitor por partição (eventos em monitor-processador.txt).
 * Como não é uma partição, não incrementa uma componente própria de Vp: faz
 * merge do vetor recebido; o líder de destino incrementa a componente local.
 */
public class AlarmProcessor {
    private static final String MONITOR_FILE = "monitor-processador.txt";

    private final int n;
    private final Partitioner partitioner;
    private final IpfsPubSub ps;
    private final Set<String> processados = ConcurrentHashMap.newKeySet();
    private final Object clockLock = new Object();
    private long lp = 0;
    private final VectorClock vp;
    private final HeartbeatMonitor[] monitores;
    private final Object logLock = new Object();

    private AlarmProcessor(int n, String api) {
        if (n < 1) throw new IllegalArgumentException("N tem de ser >= 1");
        this.n = n;
        this.partitioner = new Partitioner(n);
        this.ps = new IpfsPubSub(api);
        this.vp = VectorClock.zeros(n);
        this.monitores = new HeartbeatMonitor[n];
        for (int k = 0; k < n; k++) {
            monitores[k] = new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, this::evento);
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Uso: AlarmProcessor <N> [api]");
            return;
        }
        int n = Integer.parseInt(args[0]);
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";
        new AlarmProcessor(n, api).run();
    }

    /** Evento do detetor: consola + monitor-processador.txt, com o instante à frente. */
    private void evento(String s) {
        String linha = System.currentTimeMillis() + ";" + s;
        synchronized (logLock) {
            System.out.println(linha);
            try {
                Files.writeString(Paths.get(MONITOR_FILE), linha + System.lineSeparator(),
                        StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                System.err.println("ERRO a escrever " + MONITOR_FILE + ": " + e.getMessage());
            }
        }
    }

    private void run() {
        System.out.println("AlarmProcessor pronto (N=" + n + ", limiar=" + Wire.LIMIAR_ALARME + " C, período="
                + Wire.PERIODO_HB_MS + " ms, fator=" + Wire.FATOR_TIMEOUT + ")");
        for (int k = 0; k < n; k++) {
            final int partition = k;
            Thread t = new Thread(() -> {
                try {
                    ps.subscribe(Wire.topicData(partition), msg -> onMessage(partition, msg));
                } catch (Exception e) {
                    System.err.println("Subscricao de p" + partition + " terminou: " + e);
                }
            }, "alarm-sub-p" + k);
            t.setDaemon(false);
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
                System.err.println("Erro na verificação: " + e);
            }
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);
    }

    private void onMessage(int sourcePartition, String msg) {
        try {
            if (!"LOTE".equals(Wire.type(msg))) return;
            Batch b = Batch.decode(msg, n);
            monitores[sourcePartition].onHeartbeat(b.inc, b.hb);   // o LOTE é um heartbeat (mesmo vazio)
            evento("RECEBIDO;p" + sourcePartition + ";inc=" + b.inc + ";hb=" + b.hb + ";ultimoSeq=" + b.lastSeq
                    + ";n=" + b.records.size() + ";seq=" + (b.records.isEmpty() ? "-"
                    : b.records.get(0).getSeq() + ".." + b.records.get(b.records.size() - 1).getSeq()));
            for (SensorRecord r : b.records) {
                onRecord(sourcePartition, r);
            }
        } catch (Exception e) {
            System.err.println("Mensagem rejeitada pelo AlarmProcessor: " + e.getMessage());
        }
    }

    private void onRecord(int sourcePartition, SensorRecord r) {
        try {
            VectorClock received = r.getVector();
            if (sourcePartition < 0 || sourcePartition >= n || received.size() != n
                    || received.get(sourcePartition) != r.getSeq()) {
                throw new IllegalArgumentException("registo com particao/vetor inconsistente");
            }

            // Cada par particao:seq identifica um evento único; ignora duplicados PubSub.
            String key = sourcePartition + ":" + r.getSeq();
            if (!processados.add(key)) return;

            String alarmMessage = null;
            String alarmSensor = null;
            long observedLamport;
            VectorClock observedVector;
            synchronized (clockLock) {
                // A receção é um evento local do processador.
                lp = Math.max(lp, r.getLamport());
                // Preserva também o conhecimento de registos recebidos anteriormente.
                vp.mergeMax(received);
                observedLamport = lp;
                observedVector = vp.copy();

                // Registos ALARM-* também atualizam o conhecimento, mas não geram
                // alarmes encadeados; isso impediria ciclos de publicação.
                if (!r.getSensorId().startsWith("ALARM-")
                        && r.getTemperature() >= Wire.LIMIAR_ALARME) {
                    alarmSensor = "ALARM-" + r.getSensorId();
                    int destination = partitioner.of(alarmSensor);
                    alarmMessage = Wire.in(alarmSensor, r.getTemperature(), observedLamport, observedVector);
                    System.out.println("ALARME: " + r.getSensorId()
                            + " p" + sourcePartition + "#" + r.getSeq()
                            + " → p" + destination);
                }
            }

            if (alarmMessage != null) {
                int destination = partitioner.of(alarmSensor);
                if (monitores[destination].state() == HeartbeatMonitor.State.SUSPEITO) {
                    System.out.println("AVISO: líder p" + destination + " suspeito; alarme "
                            + alarmSensor + " pode perder-se");
                }
                ps.publish(Wire.topicIn(destination), alarmMessage);   // publica na mesma
            }
        } catch (Exception e) {
            System.err.println("Registo rejeitado pelo AlarmProcessor: " + e.getMessage());
        }
    }
}
