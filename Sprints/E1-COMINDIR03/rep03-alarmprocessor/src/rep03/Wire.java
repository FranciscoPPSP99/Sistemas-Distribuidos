package rep03;

public final class Wire {
    public static final int LIMITE_RETIDOS = 10;
    public static final int T_TCP_MS = 5000;
    public static final int PORTO_BASE = 7000;
    /** Temperatura a partir da qual o AlarmProcessor gera um alarme. */
    public static final double LIMIAR_ALARME = 28.0;

    private Wire() { }

    public static String topicIn(int k) { return "rep03-in-p" + k; }
    public static String topicData(int k) { return "rep03-p" + k; }

    /** Mensagem IN: IN;sensor;temperatura;L;V. */
    public static String in(String sensor, double temp, long lamport, VectorClock vector) {
        if (vector == null) throw new IllegalArgumentException("Vetor ausente");
        return "IN;" + sensor + ";" + temp + ";" + lamport + ";" + vector.toField();
    }

    /** Mensagem REG: REG;host;porto;seq;sensor;temperatura;timestamp;L;V. */
    public static String reg(String host, int port, SensorRecord r) {
        return "REG;" + host + ";" + port + ";" + r.toLine();
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

    /** Divide REG em {endereco, porto, linha completa do registo}. */
    public static String[] regParts(String msg) {
        String[] f = msg.trim().split(";", 4);
        if (f.length != 4 || !f[0].equals("REG"))
            throw new IllegalArgumentException("REG mal formado: " + msg);
        return new String[] { f[1], f[2], f[3] };
    }
}
