package rep01;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class PubSubReplica {
    static final String TOPIC = "rep01-temperaturas";
    static final long SILENCIO_MS = 2000; // sem mensagens durante este tempo = rajada terminada
    static final Map<Long, SensorRecord> buf = new TreeMap<>();
    
    static long seq;
    static long primeiro = 0, ultimo = 0;
    static int recebidos = 0, escritos = 0, duplicados = 0;

    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        String porto = args.length > 1 ? args[1] : "5001";
        String api = "http://127.0.0.1:" + porto + "/api/v0";
        RecordFile file = new RecordFile("replica-" + id + ".txt");

        try {
            seq = file.lastSeq();

            Thread relatorio = new Thread(() -> {
                while (true) {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        return;
                    }
                    synchronized (buf) {
                        if (recebidos > 0 && System.currentTimeMillis() - ultimo > SILENCIO_MS) {
                            System.out.println("--- Rajada terminada ---");
                            System.out.println("Primeiro recebido: " + primeiro);
                            System.out.println("Último recebido:   " + ultimo);
                            System.out.println("Latência de receção: " + (ultimo - primeiro) + " ms");
                            System.out.println("Recebidos: " + recebidos
                                    + " | Escritos: " + escritos
                                    + " | Duplicados: " + duplicados
                                    + " | Retidos em memória: " + buf.size()
                                    + " | Último seq no ficheiro: " + seq);
                            recebidos = escritos = duplicados = 0;
                            primeiro = ultimo = 0;
                        }
                    }
                }
            });
            relatorio.setDaemon(true);
            relatorio.start();

            System.out.println("Réplica " + id + " a subscrever '" + TOPIC + "' em " + api);

            new IpfsPubSub(api).subscribe(TOPIC, line -> {
                long agora = System.currentTimeMillis(); 
                SensorRecord r = parse(line);
                if (r == null) {
                    System.out.println("REJEITADO: " + line);
                    return; 
                }

                synchronized (buf) {
                    boolean sim = r.getSensorId().equals("SIM");
                    if (sim) {
                        if (recebidos == 0)
                            primeiro = agora;
                        ultimo = agora;
                        recebidos++;
                    }

                    try {
                        long n = r.getSeq();
                        if (n == seq + 1) {
                            file.append(r);
                            System.out.println("Aplicado: " + r.toLine());
                            seq++;
                            if (sim)
                                escritos++;
                            while (buf.containsKey(seq + 1)) {
                                SensorRecord retido = buf.remove(seq + 1);
                                file.append(retido);
                                System.out.println("Aplicado: " + retido.toLine());
                                seq++;
                                if (retido.getSensorId().equals("SIM"))
                                    escritos++;
                            }
                        } else if (n <= seq || buf.containsKey(n)) {
                            System.out.println("DUPLICADO: " + n);
                            if (sim)
                                duplicados++;
                        } else {
                            buf.put(n, r);
                            System.out.println("EM ESPERA: " + n + " (falta o registo " + (seq + 1) + ")");
                        }
                    } catch (IOException e) {
                        System.out.println("IO (ficheiro): " + e.getMessage());
                    }
                }
            });
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage()); // daemon parado, ligação fechada...
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.out.println("Interrompido: " + e.getMessage());
        }
    }

    private static SensorRecord parse(String line) {
        try {
            SensorRecord r = SensorRecord.fromLine(line);
            if (r.getSeq() < 1 || r.getSensorId().isBlank() || r.getTimestamp().isBlank() || !Double.isFinite(r.getTemperature())) {
                return null;
            }
            return r;
        } catch (IllegalArgumentException e) { // inclui NumberFormatException
            return null;
        }
    }
}