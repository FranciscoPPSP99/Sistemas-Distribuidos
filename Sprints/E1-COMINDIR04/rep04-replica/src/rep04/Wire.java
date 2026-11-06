package rep04;

public final class Wire {
    public static final int LIMITE_RETIDOS = 10;
    public static final int T_TCP_MS = 5000;
    public static final int PORTO_BASE = 7000;
    /** Temperatura a partir da qual o AlarmProcessor gera um alarme. */
    public static final double LIMIAR_ALARME = 28.0;

    /** Período do heartbeat (e do envio em lote), em ms. -Drep04.periodo=<ms> */
    public static final long PERIODO_HB_MS = Long.getLong("rep04.periodo", 5000L);
    /** TIMEOUT = FATOR_TIMEOUT x PERIODO_HB_MS. -Drep04.fator=<n> */
    public static final int FATOR_TIMEOUT = Integer.getInteger("rep04.fator", 3);
    /** Período da thread de verificação do detetor, em ms. */
    public static final long VERIFICACAO_MS = 1000L;

    private Wire() { }

    public static String topicIn(int k) { return "rep04-in-p" + k; }
    public static String topicData(int k) { return "rep04-p" + k; }

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