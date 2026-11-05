package rep02;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Uso: PartitionLeader <k> <N> [endereço] [api]
 *
 * Duas atividades concorrentes:
 *  - entrada: thread que subscreve rep02-in-p<k> (bloqueia no subscribe)
 *  - recuperação: thread principal, ServerSocket (bloqueia no accept e no readLine)
 * Estado partilhado: o último seq atribuído (protegido por 'lock').
 *
 * Assume que IpfsPubSub (COMINDIR01) tem:
 *   publish(String topico, String msg)
 *   subscribe(String topico, Consumer<String> handler)   // bloqueante
 * e que RecordFile tem append(SensorRecord), lastSeq() e range(from, to).
 */
public class PartitionLeader {

    private final int k;
    private final Partitioner partitioner;
    private final String host;
    private final int porto;
    private final IpfsPubSub ps;
    private final RecordFile file;

    private final Object lock = new Object();
    private long seq;   // último seq atribuído (lido e escrito pelas duas atividades)

    private PartitionLeader(int k, int n, String host, String api) throws IOException {
        this.k = k;
        this.partitioner = new Partitioner(n);
        this.host = host;
        this.porto = Wire.PORTO_BASE + k;
        this.ps = new IpfsPubSub(api);
        this.file = new RecordFile("lider-p" + k + ".txt");
        this.seq = file.lastSeq();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Uso: PartitionLeader <k> <N> [endereço] [api]");
            return;
        }
        int k = Integer.parseInt(args[0]);
        int n = Integer.parseInt(args[1]);
        String host = args.length > 2 ? args[2] : "127.0.0.1";
        String api = args.length > 3 ? args[3] : "http://127.0.0.1:5001/api/v0";
        new PartitionLeader(k, n, host, api).run();
    }

    private void run() throws IOException {
        System.out.println("Líder p" + k + " | anuncia " + host + ":" + porto + " | último seq no ficheiro: " + seq);

        // 1) abrir primeiro o porto: se estiver ocupado, falha logo aqui (BindException)
        try (ServerSocket server = new ServerSocket(porto)) {
            System.out.println("À escuta de pedidos de recuperação no porto " + porto);

            // 2) atividade de entrada
            Thread entrada = new Thread(this::escutarEntrada, "entrada-p" + k);
            entrada.setDaemon(true);
            entrada.start();

            // 3) atividade de recuperação: uma ligação de cada vez
            while (true) {
                try (Socket s = server.accept();                         // bloqueia até uma réplica se ligar
                     BufferedReader in = new BufferedReader(
                             new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                     PrintWriter out = new PrintWriter(
                             new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true)) {
                    s.setSoTimeout(Wire.T_TCP_MS);
                    atender(s, in, out);
                } catch (IOException e) {
                    System.out.println("Recuperação interrompida: " + e.getMessage());
                }
            }
        }
    }

    // ---------------------------------------------------------------- entrada (PubSub)

    private void escutarEntrada() {
        try {
            ps.subscribe(Wire.topicIn(k), this::tratarIn);   // bloqueia enquanto a subscrição durar
        } catch (Exception e) {
            System.out.println("Subscrição terminou: " + e);
            System.exit(1);
        }
    }

    private void tratarIn(String msg) {
        try {
            if (!"IN".equals(Wire.type(msg))) {
                throw new IllegalArgumentException("tipo inesperado");
            }
            String[] f = Wire.body(msg).split(";");
            if (f.length != 2 || f[0].isBlank()) {
                throw new IllegalArgumentException("IN mal formado");
            }
            String sensor = f[0].trim();
            double temp = Double.parseDouble(f[1].trim());   // NumberFormatException é IllegalArgumentException

            int j = partitioner.of(sensor);
            if (j != k) {
                System.out.println("PARTIÇÃO ERRADA: " + sensor + " pertence a p" + j);
                return;
            }

            SensorRecord r;
            synchronized (lock) {                            // atribuir seq + escrever: mesma secção
                long proximo = seq + 1;
                r = SensorRecord.fromLine(proximo + ";" + sensor + ";" + temp + ";" + System.currentTimeMillis());
                file.append(r);                              // 1.º escrever localmente...
                seq = proximo;                               // só avança se a escrita correu bem
            }
            ps.publish(Wire.topicData(k), Wire.reg(host, porto, r));   // 2.º ...depois propagar
            System.out.println("REGISTADO: " + r.toLine());
        } catch (IllegalArgumentException e) {
            System.out.println("REJEITADO: " + msg.trim());
        } catch (Exception e) {
            System.out.println("Erro a processar leitura: " + e);
        }
    }

    // ---------------------------------------------------------------- recuperação (TCP)

    private void atender(Socket s, BufferedReader in, PrintWriter out) throws IOException {
        String pedido = in.readLine();                       // bloqueia até chegar o pedido (ou timeout)
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

        long ultimo;
        synchronized (lock) {
            ultimo = seq;
        }
        ate = Math.min(ate, ultimo);

        List<SensorRecord> regs = file.range(de, ate);       // o ficheiro do líder é a fonte
        for (SensorRecord r : regs) {
            out.println(r.toLine());
        }
        out.println("FIM");
        System.out.println("RECUPERAÇÃO: " + de + ".." + ate + " para " + s.getRemoteSocketAddress()
                + " (" + regs.size() + " registos)");
    }
}
