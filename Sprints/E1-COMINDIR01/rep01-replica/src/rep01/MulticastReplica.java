package rep01;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;

public class MulticastReplica {
    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;
    static final int SILENCIO_MS = 2000;

    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        RecordFile file = new RecordFile("replica-" + id + ".txt");
        Map<Long, SensorRecord> buf = new TreeMap<>();

        long primeiro = 0, ultimo = 0;
        int recebidos = 0, escritos = 0, duplicados = 0;

        try (MulticastSocket socket = new MulticastSocket(PORT)) {
            long seq = file.lastSeq();                   
            InetAddress group = InetAddress.getByName(GROUP);
            socket.joinGroup(group);
            socket.setSoTimeout(SILENCIO_MS);
            System.out.println("Réplica " + id + " à escuta em " + GROUP + ":" + PORT);

            byte[] buffer = new byte[1000];
            while (true) {
                DatagramPacket p = new DatagramPacket(buffer, buffer.length);
                try {
                    socket.receive(p);                       // bloqueia até chegar um datagrama
                } catch (SocketTimeoutException e) {
                    // silêncio: se houve rajada, imprime as estatísticas e reinicia os contadores
                    if (recebidos > 0) {
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
                    continue;
                }
                long agora = System.currentTimeMillis();
                String line = new String(p.getData(), 0, p.getLength(), StandardCharsets.UTF_8);

                SensorRecord r = parse(line);
                if (r == null) {
                    System.out.println("REJEITADO: " + line);
                    continue;                            
                }

                boolean sim = r.getSensorId().equals("SIM");
                if (sim) {
                    if (recebidos == 0) primeiro = agora;
                    ultimo = agora;
                    recebidos++;
                }

                long n = r.getSeq();
                if (n == seq + 1) {
                    file.append(r);
                    System.out.println("Aplicado: " + r.toLine());
                    seq++;
                    if (sim) escritos++;
                    while (buf.containsKey(seq + 1)) {
                        SensorRecord retido = buf.remove(seq + 1);
                        file.append(retido);
                        System.out.println("Aplicado: " + retido.toLine());
                        seq++;
                        if (retido.getSensorId().equals("SIM")) escritos++;
                    }
                } else if (n <= seq || buf.containsKey(n)) {
                    System.out.println("DUPLICADO: " + n);
                    if (sim) duplicados++;
                } else {
                    buf.put(n, r);
                    System.out.println("EM ESPERA: " + n + " (falta o registo " + (seq + 1) + ")");
                }
            }
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }

    private static SensorRecord parse(String line) {
        try {
            SensorRecord r = SensorRecord.fromLine(line);   
            if (r.getSeq() < 1 || r.getSensorId().isBlank() || r.getTimestamp().isBlank() || !Double.isFinite(r.getTemperature())) {
                return null;
            }
            return r;
        } catch (IllegalArgumentException e) {           // inclui NumberFormatException
            return null;
        }
    }
}