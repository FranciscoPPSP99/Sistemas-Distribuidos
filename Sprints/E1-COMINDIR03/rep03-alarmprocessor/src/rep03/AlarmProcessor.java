package rep03;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Subscreve todas as partições. Mantém o conhecimento causal acumulado que
 * observou e publica ALARM-<sensor> quando recebe uma leitura >= LIMIAR_ALARME.
 * Como não é uma partição, não incrementa uma componente própria de Vp: faz
 * merge do vetor recebido; o líder de destino incrementa a componente local.
 */
public class AlarmProcessor {
    private final int n;
    private final Partitioner partitioner;
    private final IpfsPubSub ps;
    private final Set<String> processados = ConcurrentHashMap.newKeySet();
    private final Object clockLock = new Object();
    private long lp = 0;
    private final VectorClock vp;

    private AlarmProcessor(int n, String api) {
        if (n < 1) throw new IllegalArgumentException("N tem de ser >= 1");
        this.n = n;
        this.partitioner = new Partitioner(n);
        this.ps = new IpfsPubSub(api);
        this.vp = VectorClock.zeros(n);
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

    private void run() {
        System.out.println("AlarmProcessor pronto (N=" + n + ", limiar=" + Wire.LIMIAR_ALARME + " C)");
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
    }

    private void onMessage(int sourcePartition, String msg) {
        try {
            if (!"REG".equals(Wire.type(msg))) return;
            String[] parts = Wire.regParts(msg);
            SensorRecord r = SensorRecord.fromLine(parts[2]);
            VectorClock received = r.getVector();
            if (sourcePartition < 0 || sourcePartition >= n || received.size() != n
                    || received.get(sourcePartition) != r.getSeq()) {
                throw new IllegalArgumentException("REG com particao/vetor inconsistente");
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
                ps.publish(Wire.topicIn(destination), alarmMessage);
            }
        } catch (Exception e) {
            System.err.println("Mensagem rejeitada pelo AlarmProcessor: " + e.getMessage());
        }
    }
}
