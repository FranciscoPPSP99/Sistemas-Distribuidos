package rep04;

import java.util.ArrayList;
import java.util.List;

public final class Batch {
    public final String address;
    public final int port;
    public final long inc;
    public final long hb;
    public final long lastSeq;
    public final List<SensorRecord> records;

    public Batch(String address, int port, long inc, long hb, long lastSeq, List<SensorRecord> records) {
        this.address = address;
        this.port = port;
        this.inc = inc;
        this.hb = hb;
        this.lastSeq = lastSeq;
        this.records = List.copyOf(records);
    }

    public String encode() {
        StringBuilder sb = new StringBuilder();
        sb.append("LOTE;").append(address).append(';').append(port).append(';')
          .append(inc).append(';').append(hb).append(';').append(lastSeq).append(';')
          .append(records.size());
        for (SensorRecord r : records) {
            sb.append('\n').append(r.toLine());
        }
        return sb.toString();
    }

    /** Interpreta um LOTE. n = nº de partições (dimensão esperada dos vetores; n <= 0 não valida a dimensão). */
    public static Batch decode(String msg, int n) {
        String[] lines = msg.strip().split("\\R");
        String[] h = lines[0].trim().split(";", -1);
        if (h.length != 7 || !h[0].equals("LOTE")) {
            throw new IllegalArgumentException("Cabeçalho LOTE mal formado: " + lines[0]);
        }
        int count = Integer.parseInt(h[6].trim());
        if (count < 0 || lines.length - 1 != count) {
            throw new IllegalArgumentException("LOTE declara n=" + count + " mas tem " + (lines.length - 1) + " linhas");
        }
        List<SensorRecord> recs = new ArrayList<>();
        for (int i = 1; i < lines.length; i++) {
            SensorRecord r = SensorRecord.fromLine(lines[i]);
            if (n > 0 && r.getVector().size() != n) {
                throw new IllegalArgumentException("vetor com dimensão errada no registo " + r.getSeq());
            }
            recs.add(r);
        }
        return new Batch(h[1], Integer.parseInt(h[2].trim()), Long.parseLong(h[3].trim()),
                Long.parseLong(h[4].trim()), Long.parseLong(h[5].trim()), recs);
    }

    /** Só o cabeçalho  */
    public static Batch decodeHeader(String msg) {
        String[] lines = msg.strip().split("\\R", 2);
        String[] h = lines[0].trim().split(";", -1);
        if (h.length != 7 || !h[0].equals("LOTE")) {
            throw new IllegalArgumentException("Cabeçalho LOTE mal formado: " + lines[0]);
        }
        return new Batch(h[1], Integer.parseInt(h[2].trim()), Long.parseLong(h[3].trim()),
                Long.parseLong(h[4].trim()), Long.parseLong(h[5].trim()), List.of());
    }
}