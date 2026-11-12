package raft04;

import java.util.ArrayList;
import java.util.List;

public final class Batch {
    public final long term;
    public final int leader;
    public final String address;
    public final int port;
    public final long hb;
    public final long lastSeq;
    public final long lastTerm;
    public final List<SensorRecord> records;

    public Batch(long term, int leader, String address, int port, long hb, long lastSeq, long lastTerm, List<SensorRecord> records) {
        this.term = term;
        this.leader = leader;
        this.address = address;
        this.port = port;
        this.hb = hb;
        this.lastSeq = lastSeq;
        this.lastTerm = lastTerm;
        this.records = List.copyOf(records);
    }

    public String encode() {
        StringBuilder sb = new StringBuilder();
        sb.append("LOTE;").append(term).append(';').append(leader).append(';')
          .append(address).append(';').append(port).append(';').append(hb).append(';')
          .append(lastSeq).append(';').append(lastTerm).append(';').append(records.size());
        for (SensorRecord r : records) {
            sb.append('\n').append(r.toLine());
        }
        return sb.toString();
    }

    /** Interpreta um LOTE. n = nº de partições (dimensão esperada dos vetores; n <= 0 não valida a dimensão). */
    public static Batch decode(String msg, int n) {
        String[] lines = msg.strip().split("\\R");
        String[] h = lines[0].trim().split(";", -1);
        if (h.length != 9 || !h[0].equals("LOTE")) {
            throw new IllegalArgumentException("Cabeçalho LOTE mal formado: " + lines[0]);
        }
        int count = Integer.parseInt(h[8].trim());
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
        return new Batch(Long.parseLong(h[1].trim()), Integer.parseInt(h[2].trim()), h[3], Integer.parseInt(h[4].trim()),
                Long.parseLong(h[5].trim()), Long.parseLong(h[6].trim()), Long.parseLong(h[7].trim()), recs);
    }

    /** Só o cabeçalho  */
    public static Batch decodeHeader(String msg) {
        String[] lines = msg.strip().split("\\R", 2);
        String[] h = lines[0].trim().split(";", -1);
        if (h.length != 9 || !h[0].equals("LOTE")) {
            throw new IllegalArgumentException("Cabeçalho LOTE mal formado: " + lines[0]);
        }
        return new Batch(Long.parseLong(h[1].trim()), Integer.parseInt(h[2].trim()), h[3], Integer.parseInt(h[4].trim()),
                Long.parseLong(h[5].trim()), Long.parseLong(h[6].trim()), Long.parseLong(h[7].trim()), List.of());
    }
}