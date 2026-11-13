package raft04;
 
import java.nio.file.Files;
import java.nio.file.Paths;
 
public final class Wire {
    public static final int LIMITE_RETIDOS = 10;
    public static final int T_TCP_MS = 5000;
    public static final int PORTO_BASE = 7000;
    /** Temperatura a partir da qual o AlarmProcessor gera um alarme. */
    public static final double LIMIAR_ALARME = 28.0;
 
    /** Período do heartbeat (e do envio em lote), em ms. -Draft04.periodo=<ms> */
    public static final long PERIODO_HB_MS = Long.getLong("raft04.periodo", 2000L);
    /** TIMEOUT = FATOR_TIMEOUT x PERIODO_HB_MS. -Draft04.fator=<n> */
    public static final int FATOR_TIMEOUT = Integer.getInteger("raft04.fator", 3);
    /** Período da thread de verificação (detetor e prazo de eleição), em ms. */
    public static final long VERIFICACAO_MS = 200L;
 
    /** Limites do prazo de eleição, em ms. -Draft04.eleicaoMin=<ms> -Draft04.eleicaoMax=<ms> */
    public static final long ELEICAO_MIN_MS = Long.getLong("raft04.eleicaoMin", 3 * PERIODO_HB_MS);
    public static final long ELEICAO_MAX_MS = Long.getLong("raft04.eleicaoMax", 6 * PERIODO_HB_MS);
    /** Nós por partição de dados. */
    public static final int GRUPO = 3;
 
    private Wire() { }
 
    public static String topicIn(int k) { return "raft04-in-p" + k; }
    public static String topicData(int k) { return "raft04-p" + k; }
    public static String topicElection(int k) { return "raft04-eleicao-p" + k; }
 
    /** Porto TCP de recuperação do nó i (1..GRUPO) da partição k. */
    public static int nodePort(int k, int i) { return PORTO_BASE + k * 10 + i; }
 
    public static boolean isolated(int k, int i) {
        return Files.exists(Paths.get("isolar-p" + k + "-n" + i));
    }
 
    /** Mensagem IN: IN;sensor;temperatura;L;V. */
    public static String in(String sensor, double temp, long lamport, VectorClock vector) {
        if (vector == null) throw new IllegalArgumentException("Vetor ausente");
        return "IN;" + sensor + ";" + temp + ";" + lamport + ";" + vector.toField();
    }
 
    public static String type(String msg) {
        String m = msg.trim();
        int i = m.indexOf(';');
        if (i <= 0) throw new IllegalArgumentException("Mensagem sem tipo: " + msg);
        return m.substring(0, i);
    }
 
    public static String body(String msg) {
        String m = msg.trim();
        int i = m.indexOf(';');
        if (i < 0) throw new IllegalArgumentException("Mensagem sem corpo: " + msg);
        return m.substring(i + 1);
    }
}
