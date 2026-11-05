package rep03;

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
 *  - entrada: thread que subscreve rep03-in-p<k> (bloqueia no subscribe)
 *  - recuperação: thread principal, ServerSocket (bloqueia no accept e no readLine)
 * Estado partilhado: o último seq atribuído (protegido por 'lock').
 *
 * Assume que IpfsPubSub (COMINDIR03) tem:
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
    private long seq;   // ultimo seq atribuido nesta particao
    private long lamport;
    private VectorClock vector;

    private PartitionLeader(int k, int n, String host, String api) throws IOException {
        this.k = k;
        this.partitioner = new Partitioner(n);
        this.host = host;
        this.porto = Wire.PORTO_BASE + k;
        this.ps = new IpfsPubSub(api);
        this.file = new RecordFile("lider-p" + k + ".txt");
        SensorRecord last = file.last();
        if (last == null) {
            this.seq = 0;
            this.lamport = 0;
            this.vector = VectorClock.zeros(n);
        } else {
            this.seq = last.getSeq();
            this.lamport = last.getLamport();
            this.vector = last.getVector();
            if (this.vector.size() != n) {
                throw new IOException("O vetor guardado tem " + this.vector.size()
                        + " componentes, mas N=" + n + "; use ficheiros novos ou a configuracao original.");
            }
        }
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
                System.out.println("PARTIÇÃO ERRADA: " + sensor + " pertence a p" + j);
                return;
            }

            SensorRecord r;
            synchronized (lock) {
                // Calcular em copias: so alterar o estado do lider depois de gravar com sucesso.
                long nextSeq = seq + 1;
                long nextLamport = Math.max(lamport, incomingLamport) + 1;
                VectorClock nextVector = vector.copy();
                nextVector.mergeMax(incomingVector);
                nextVector.increment(k);

                // Invariante exigida: a componente local do vetor coincide com seq.
                if (nextVector.get(k) != nextSeq) {
                    throw new IllegalStateException("Invariante causal violada: V[" + k + "]="
                            + nextVector.get(k) + ", mas seq=" + nextSeq
                            + ". Verifique os carimbos recebidos e o estado persistido.");
                }

                r = new SensorRecord(nextSeq, sensor, temp,
                        Long.toString(System.currentTimeMillis()), nextLamport, nextVector);
                file.append(r); // persistir antes de confirmar o novo estado local
                seq = nextSeq;
                lamport = nextLamport;
                vector = nextVector;
            }
            ps.publish(Wire.topicData(k), Wire.reg(host, porto, r));
            System.out.println("EVENTO: p" + k + " seq=" + r.getSeq() + " sensor=" + r.getSensorId()
                    + " temperatura=" + r.getTemperature() + " L=" + r.getLamport()
                    + " V=" + r.getVector());
        } catch (IllegalArgumentException e) {
            System.out.println("REJEITADO: " + msg.trim() + " (" + e.getMessage() + ")");
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
