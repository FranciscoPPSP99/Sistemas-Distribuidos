package raft04;

/** Registo persistente e mensagem de dados: seq;sensor;temperatura;timestamp;L;V. */
public final class SensorRecord {
    private final long seq;
    private final String sensorId;
    private final double temperature;
    private final String timestamp;
    private final long lamport;
    private final VectorClock vector;
    private final long term;

    public SensorRecord(long seq, long term, String sensorId, double temperature, String timestamp,
                        long lamport, VectorClock vector) {
        if (seq < 1) throw new IllegalArgumentException("seq tem de ser >= 1");
        if (sensorId == null || sensorId.isBlank()) throw new IllegalArgumentException("sensor vazio");
        if (timestamp == null || timestamp.isBlank()) throw new IllegalArgumentException("timestamp vazio");
        if (lamport < 0) throw new IllegalArgumentException("Relogio de Lamport negativo");
        if (vector == null) throw new IllegalArgumentException("Relogio vetorial ausente");
        this.seq = seq;
        this.term = term;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.timestamp = timestamp;
        this.lamport = lamport;
        this.vector = vector.copy();
        
    }

    public String toLine() {
        return seq + ";" + term + ";" + sensorId + ";" + temperature + ";" + timestamp
                + ";" + lamport + ";" + vector.toField() ;
    }

    public static SensorRecord fromLine(String line) {
        String[] f = line.trim().split(";", -1);
        if (f.length != 7) {
            throw new IllegalArgumentException("Registo COMINDIR03 mal formado (esperava 6 campos): " + line);
        }
        return new SensorRecord(Long.parseLong(f[0]), Long.parseLong(f[1]), f[2], Double.parseDouble(f[3]), f[4],
                Long.parseLong(f[5]), VectorClock.parse(f[6]));
    }

    public long getSeq() { return seq; }
    public String getSensorId() { return sensorId; }
    public double getTemperature() { return temperature; }
    public String getTimestamp() { return timestamp; }
    public long getLamport() { return lamport; }
    public VectorClock getVector() { return vector.copy(); }
    public long getTerm() { return term; }
}
