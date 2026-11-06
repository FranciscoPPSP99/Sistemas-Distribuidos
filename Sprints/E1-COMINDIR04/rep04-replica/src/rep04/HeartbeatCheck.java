package rep04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Uso: HeartbeatCheck <N> <pasta> [falhas.txt]
 *
 * Lê hb-p*.txt, lider-p*.txt e monitor-*.txt de uma pasta e escreve um relatório
 * (consola + relatorio-heartbeat.txt na pasta). falhas.txt tem linhas "p<k>;instante"
 * (instante em ms desde 1970, anotado no momento do kill).
 *
 * Usar uma pasta NOVA por ensaio: os ficheiros são acrescentados a cada execução.
 * Os instantes dos vários ficheiros só são comparáveis se tudo correr na mesma máquina.
 */
public final class HeartbeatCheck {
    private HeartbeatCheck() { }

    private static final StringBuilder OUT = new StringBuilder();

    private static void pl(String s) {
        OUT.append(s).append('\n');
    }

    private static String f(String fmt, Object... a) {
        return String.format(Locale.ROOT, fmt, a);
    }

    // ------------------------------------------------------------------ modelos

    /** Linha de hb-p<k>.txt: inc;hb;instante;ultimoSeq;n;bytes */
    private static final class Hb {
        long inc;
        long hb;
        long t;
        long ultimoSeq;
        int n;
        int bytes;
    }

    /** Linha de monitor-<id>.txt: instante;evento;p<k>;detalhe */
    private static final class Ev {
        long t;
        String name;
        int p;
        String det = "";
        final Map<String, String> kv = new HashMap<>();
        long rangeA = -1;   // só em RECEBIDO com registos
        long rangeB = -1;
    }

    private static final class Stat {
        int n;
        double sum;
        double min = Double.MAX_VALUE;
        double max = -Double.MAX_VALUE;

        void add(double v) {
            n++;
            sum += v;
            min = Math.min(min, v);
            max = Math.max(max, v);
        }

        double avg() {
            return n == 0 ? 0 : sum / n;
        }

        String fmt(String unit) {
            if (n == 0) return "n/d";
            return f("n=%d média=%.1f%s máx=%.0f%s", n, avg(), unit, max, unit);
        }
    }

    private static final class Suspeita {
        String participante;
        int p;
        long t;
        long silencioLocalMs = -1;
        long incAntes = -1;
        long silencioPublicadoMs = -1;
        boolean falsa;
    }

    // ------------------------------------------------------------------ leitura

    private static List<Hb> lerHb(Path folder, int k) throws IOException {
        List<Hb> out = new ArrayList<>();
        Path p = folder.resolve("hb-p" + k + ".txt");
        if (!Files.exists(p)) return out;
        for (String line : Files.readAllLines(p, StandardCharsets.UTF_8)) {
            if (line.isBlank()) continue;
            String[] x = line.trim().split(";", -1);
            if (x.length != 6) {
                pl("AVISO: linha mal formada em " + p.getFileName() + ": " + line);
                continue;
            }
            Hb h = new Hb();
            h.inc = Long.parseLong(x[0]);
            h.hb = Long.parseLong(x[1]);
            h.t = Long.parseLong(x[2]);
            h.ultimoSeq = Long.parseLong(x[3]);
            h.n = Integer.parseInt(x[4]);
            h.bytes = Integer.parseInt(x[5]);
            out.add(h);
        }
        return out;
    }

    private static List<SensorRecord> lerLider(Path folder, int k) throws IOException {
        List<SensorRecord> out = new ArrayList<>();
        Path p = folder.resolve("lider-p" + k + ".txt");
        if (!Files.exists(p)) return out;
        for (String line : Files.readAllLines(p, StandardCharsets.UTF_8)) {
            if (line.isBlank()) continue;
            try {
                out.add(SensorRecord.fromLine(line));
            } catch (RuntimeException e) {
                pl("AVISO: registo mal formado em " + p.getFileName() + ": " + line);
            }
        }
        return out;
    }

    private static Ev parseEv(String line) {
        String[] x = line.trim().split(";", 4);
        if (x.length < 3 || !x[2].startsWith("p")) return null;
        Ev e = new Ev();
        e.t = Long.parseLong(x[0]);
        e.name = x[1];
        e.p = Integer.parseInt(x[2].substring(1));
        e.det = x.length > 3 ? x[3] : "";
        for (String kv : e.det.split(";")) {
            int i = kv.indexOf('=');
            if (i > 0) e.kv.put(kv.substring(0, i), kv.substring(i + 1));
        }
        String seq = e.kv.get("seq");
        if (e.name.equals("RECEBIDO") && seq != null && seq.contains("..")) {
            String[] r = seq.split("\\.\\.");
            e.rangeA = Long.parseLong(r[0]);
            e.rangeB = Long.parseLong(r[1]);
        }
        return e;
    }

    private static long num(Ev e, String key, long def) {
        String v = e.kv.get(key);
        if (v == null) return def;
        try {
            return Long.parseLong(v);
        } catch (NumberFormatException ex) {
            return def;
        }
    }

    // ------------------------------------------------------------------ main

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Uso: HeartbeatCheck <N> <pasta> [falhas.txt]");
            return;
        }
        int n = Integer.parseInt(args[0]);
        if (n < 1) throw new IllegalArgumentException("N tem de ser >= 1");
        Path folder = Paths.get(args[1]);
        long delta = Wire.PERIODO_HB_MS;
        long timeout = delta * Wire.FATOR_TIMEOUT;
        String host = System.getProperty("rep04.host", "127.0.0.1");

        pl("=== HeartbeatCheck | N=" + n + " | pasta=" + folder + " ===");
        pl("Parâmetros assumidos: Δ=" + delta + " ms, FATOR_TIMEOUT=" + Wire.FATOR_TIMEOUT
                + " (TIMEOUT=" + timeout + " ms), VERIFICACAO=" + Wire.VERIFICACAO_MS + " ms");
        pl("(se os processos correram com outro -Drep04.periodo / -Drep04.fator, passe os mesmos -D aqui)");
        pl("");

        // dados
        List<List<Hb>> hbs = new ArrayList<>();
        List<List<SensorRecord>> lider = new ArrayList<>();
        for (int k = 0; k < n; k++) {
            hbs.add(lerHb(folder, k));
            lider.add(lerLider(folder, k));
        }
        Map<String, List<Ev>> monitores = new TreeMap<>();
        try (Stream<Path> s = Files.list(folder)) {
            List<Path> files = s.filter(p -> p.getFileName().toString().startsWith("monitor-")
                    && p.getFileName().toString().endsWith(".txt"))
                    .sorted().collect(Collectors.toList());
            for (Path p : files) {
                String name = p.getFileName().toString();
                String id = name.substring("monitor-".length(), name.length() - ".txt".length());
                List<Ev> evs = new ArrayList<>();
                for (String line : Files.readAllLines(p, StandardCharsets.UTF_8)) {
                    if (line.isBlank()) continue;
                    try {
                        Ev e = parseEv(line);
                        if (e != null) evs.add(e);
                    } catch (RuntimeException ex) {
                        pl("AVISO: evento mal formado em " + name + ": " + line);
                    }
                }
                evs.sort(Comparator.comparingLong(e -> e.t));
                monitores.put(id, evs);
            }
        }
        pl("Participantes (monitor-*.txt): " + (monitores.isEmpty() ? "nenhum" : String.join(", ", monitores.keySet())));
        pl("");

        // 1. heartbeats e lotes
        pl("## 1. Heartbeats e lotes, por partição e incarnação");
        long totalLotes = 0;
        long totalVazios = 0;
        for (int k = 0; k < n; k++) {
            List<Hb> l = hbs.get(k);
            if (l.isEmpty()) {
                pl("p" + k + ": sem heartbeats (hb-p" + k + ".txt ausente ou vazio)");
                continue;
            }
            Map<Long, List<Hb>> porInc = new LinkedHashMap<>();
            for (Hb h : l) porInc.computeIfAbsent(h.inc, x -> new ArrayList<>()).add(h);
            for (Map.Entry<Long, List<Hb>> en : porInc.entrySet()) {
                List<Hb> hs = en.getValue();
                hs.sort(Comparator.comparingLong(h -> h.hb));
                int vazios = 0;
                Stat regs = new Stat();
                Stat ivl = new Stat();
                for (int i = 0; i < hs.size(); i++) {
                    Hb h = hs.get(i);
                    if (h.n == 0) vazios++;
                    regs.add(h.n);
                    if (i > 0) ivl.add(h.t - hs.get(i - 1).t);
                }
                double minutos = hs.size() > 1 ? (hs.get(hs.size() - 1).t - hs.get(0).t) / 60000.0 : 0;
                totalLotes += hs.size();
                totalVazios += vazios;
                pl(f("p%d inc=%d: %d lotes (%.1f/min) | vazios %d (%.0f%%) | registos/lote média=%.2f máx=%.0f",
                        k, en.getKey(), hs.size(), minutos > 0 ? (hs.size() - 1) / minutos : 0.0,
                        vazios, 100.0 * vazios / hs.size(), regs.avg(), regs.max));
                pl("      intervalo entre heartbeats: " + ivl.fmt(" ms") + " (Δ=" + delta + " ms)");
            }
        }
        if (totalLotes > 0) {
            pl(f("Total: %d lotes, %d vazios (%.0f%%)", totalLotes, totalVazios, 100.0 * totalVazios / totalLotes));
        }
        pl("");

        // 2. suspeitas
        pl("## 2. Suspeitas por participante e partição");
        List<Suspeita> suspeitas = new ArrayList<>();
        for (Map.Entry<String, List<Ev>> en : monitores.entrySet()) {
            List<Ev> evs = en.getValue();
            for (Ev e : evs) {
                if (!e.name.equals("SUSPEITA")) continue;
                Suspeita s = new Suspeita();
                s.participante = en.getKey();
                s.p = e.p;
                s.t = e.t;
                s.silencioLocalMs = num(e, "silencio_ms", -1);
                // incarnação do último heartbeat recebido antes da suspeita
                for (Ev r : evs) {
                    if (r.t <= e.t && r.p == e.p && r.name.equals("RECEBIDO")) s.incAntes = num(r, "inc", -1);
                }
                // primeira retoma depois da suspeita
                for (Ev r : evs) {
                    if (r.t >= e.t && r.p == e.p && r.name.equals("RETOMA")) {
                        s.falsa = s.incAntes != -1 && num(r, "inc", -2) == s.incAntes;
                        break;
                    }
                }
                // silêncio desde o último heartbeat PUBLICADO (hb-p<k>.txt)
                if (e.p >= 0 && e.p < n) {
                    long ultimo = -1;
                    for (Hb h : hbs.get(e.p)) {
                        if ((s.incAntes == -1 || h.inc == s.incAntes) && h.t <= e.t) ultimo = Math.max(ultimo, h.t);
                    }
                    if (ultimo >= 0) s.silencioPublicadoMs = e.t - ultimo;
                }
                suspeitas.add(s);
            }
        }
        int reais = 0;
        int falsas = 0;
        if (suspeitas.isEmpty()) pl("Nenhuma suspeita registada.");
        for (Suspeita s : suspeitas) {
            if (s.falsa) falsas++;
            else reais++;
            pl(f("%s p%d @%d: silêncio observado=%s ms | desde o último heartbeat publicado=%s ms | %s",
                    s.participante, s.p, s.t,
                    s.silencioLocalMs < 0 ? "n/d" : Long.toString(s.silencioLocalMs),
                    s.silencioPublicadoMs < 0 ? "n/d" : Long.toString(s.silencioPublicadoMs),
                    s.falsa ? "FALSA SUSPEITA (retoma com o mesmo inc=" + s.incAntes + ")"
                            : "FALHA REAL (sem novo heartbeat da incarnação " + (s.incAntes == -1 ? "?" : s.incAntes) + ")"));
        }
        pl("Total: " + suspeitas.size() + " suspeitas = " + reais + " falhas reais + " + falsas + " falsas suspeitas");
        pl("");

        // 3. tempo de deteção
        pl("## 3. Tempo de deteção");
        Stat deteccao = new Stat();
        if (args.length < 3) {
            pl("(sem falhas.txt: indique-o como 3.º argumento para calcular os tempos de deteção)");
        } else {
            List<long[]> falhasLista = new ArrayList<>();
            for (String line : Files.readAllLines(Paths.get(args[2]), StandardCharsets.UTF_8)) {
                String t = line.trim();
                if (t.isEmpty() || t.startsWith("#")) continue;
                String[] x = t.split(";");
                if (x.length != 2 || !x[0].startsWith("p")) {
                    pl("AVISO: linha de falhas.txt ignorada: " + line);
                    continue;
                }
                falhasLista.add(new long[] { Long.parseLong(x[0].substring(1)), Long.parseLong(x[1].trim()) });
            }
            long lo = timeout - delta;
            long hi = timeout + Wire.VERIFICACAO_MS;
            pl("Intervalo previsto: ]" + lo + ", " + hi + "] ms (TIMEOUT-Δ, TIMEOUT+VERIFICACAO)");
            for (long[] fl : falhasLista) {
                int p = (int) fl[0];
                long tf = fl[1];
                long proxima = Long.MAX_VALUE;   // próxima falha da mesma partição
                for (long[] o : falhasLista) {
                    if (o[0] == p && o[1] > tf) proxima = Math.min(proxima, o[1]);
                }
                pl("Falha de p" + p + " @" + tf + ":");
                for (Map.Entry<String, List<Ev>> en : monitores.entrySet()) {
                    boolean segue = false;
                    for (Ev e : en.getValue()) {
                        if (e.p == p) {
                            segue = true;
                            break;
                        }
                    }
                    if (!segue) continue;
                    Suspeita achada = null;
                    for (Suspeita s : suspeitas) {
                        if (s.participante.equals(en.getKey()) && s.p == p && s.t >= tf && s.t < proxima) {
                            achada = s;
                            break;
                        }
                    }
                    if (achada == null) {
                        pl("   " + en.getKey() + ": NÃO DETETADA");
                    } else if (achada.falsa) {
                        pl("   " + en.getKey() + ": suspeita @" + achada.t + " classificada como falsa (não conta)");
                    } else {
                        long d = achada.t - tf;
                        deteccao.add(d);
                        pl(f("   %s: %d ms (%.2f s) %s", en.getKey(), d, d / 1000.0,
                                (d > lo && d <= hi) ? "dentro do intervalo" : "FORA do intervalo previsto"));
                    }
                }
            }
            if (deteccao.n > 0) {
                pl(f("Resumo: n=%d mínimo=%.2f s médio=%.2f s máximo=%.2f s",
                        deteccao.n, deteccao.min / 1000.0, deteccao.avg() / 1000.0, deteccao.max / 1000.0));
            }
        }
        pl("");

        // 4. latência de lote
        pl("## 4. Latência de lote (instante do RECEBIDO que contém o registo - instante do registo)");
        pl("   (compara relógios de processos diferentes: válido porque tudo corre na mesma máquina)");
        for (int k = 0; k < n; k++) {
            List<SensorRecord> recs = lider.get(k);
            if (recs.isEmpty()) {
                pl("p" + k + ": sem registos no líder");
                continue;
            }
            Stat pooled = new Stat();
            Map<String, Stat> porPart = new TreeMap<>();
            int semRecebido = 0;
            for (SensorRecord r : recs) {
                long ts;
                try {
                    ts = Long.parseLong(r.getTimestamp());
                } catch (NumberFormatException e) {
                    continue;
                }
                boolean visto = false;
                for (Map.Entry<String, List<Ev>> en : monitores.entrySet()) {
                    for (Ev e : en.getValue()) {
                        if (e.p == k && e.name.equals("RECEBIDO") && e.rangeA >= 0
                                && r.getSeq() >= e.rangeA && r.getSeq() <= e.rangeB) {
                            double lat = e.t - ts;
                            pooled.add(lat);
                            porPart.computeIfAbsent(en.getKey(), x -> new Stat()).add(lat);
                            visto = true;
                            break;   // primeira receção deste participante
                        }
                    }
                }
                if (!visto) semRecebido++;
            }
            pl("p" + k + ": " + pooled.fmt(" ms") + " (Δ/2=" + delta / 2 + " ms)"
                    + (semRecebido > 0 ? " | " + semRecebido + " registos sem RECEBIDO (recuperados por TCP ou de outro ensaio)" : ""));
            for (Map.Entry<String, Stat> en : porPart.entrySet()) {
                pl("      " + en.getKey() + ": " + en.getValue().fmt(" ms"));
            }
        }
        pl("");

        // 5. eventos
        pl("## 5. HB_PERDIDO e RECUPERACAO");
        long totalHbPerdido = 0;
        long totalRecup = 0;
        for (Map.Entry<String, List<Ev>> en : monitores.entrySet()) {
            Map<Integer, int[]> c = new TreeMap<>();
            for (Ev e : en.getValue()) {
                if (e.name.equals("HB_PERDIDO")) {
                    c.computeIfAbsent(e.p, x -> new int[2])[0]++;
                    totalHbPerdido++;
                } else if (e.name.equals("RECUPERACAO")) {
                    c.computeIfAbsent(e.p, x -> new int[2])[1]++;
                    totalRecup++;
                }
            }
            if (c.isEmpty()) continue;
            for (Map.Entry<Integer, int[]> x : c.entrySet()) {
                pl("   " + en.getKey() + " p" + x.getKey() + ": HB_PERDIDO=" + x.getValue()[0]
                        + " RECUPERACAO=" + x.getValue()[1]);
            }
        }
        pl("Total: HB_PERDIDO=" + totalHbPerdido + " RECUPERACAO=" + totalRecup);
        pl("");

        // 6. bytes e mensagens
        pl("## 6. Mensagens e bytes por registo (LOTE vs REG da COMINDIR03)");
        pl("   REG equivalente = \"REG;" + host + ";<porto>;\" + linha do registo (host: -Drep04.host)");
        long somaLotes = 0;
        long somaRegs = 0;
        for (int k = 0; k < n; k++) {
            List<Hb> l = hbs.get(k);
            if (l.isEmpty()) continue;
            long bytesComRegs = 0;
            long bytesTodos = 0;
            long regsEmLotes = 0;
            for (Hb h : l) {
                bytesTodos += h.bytes;
                if (h.n > 0) {
                    bytesComRegs += h.bytes;
                    regsEmLotes += h.n;
                }
            }
            long regBytes = 0;
            for (SensorRecord r : lider.get(k)) {
                regBytes += ("REG;" + host + ";" + (Wire.PORTO_BASE + k) + ";" + r.toLine())
                        .getBytes(StandardCharsets.UTF_8).length;
            }
            double regMedio = lider.get(k).isEmpty() ? 0 : (double) regBytes / lider.get(k).size();
            somaLotes += l.size();
            somaRegs += regsEmLotes;
            pl(f("p%d: %d lotes publicados vs %d registos (=%d REG na COMINDIR03)", k, l.size(), regsEmLotes, regsEmLotes));
            if (regsEmLotes > 0) {
                pl(f("      bytes/registo em LOTE (lotes não vazios, com a parte do cabeçalho)=%.1f | contando também os lotes vazios=%.1f | REG individual=%.1f",
                        (double) bytesComRegs / regsEmLotes, (double) bytesTodos / regsEmLotes, regMedio));
            }
        }
        pl("Total no tópico de dados: COMINDIR03=" + somaRegs + " mensagens | COMINDIR04=" + somaLotes + " mensagens");
        pl("");
        pl("(Corra também CausalCheck " + n + " <pasta> para confirmar V[k] = seq.)");

        System.out.print(OUT);
        Files.writeString(folder.resolve("relatorio-heartbeat.txt"), OUT.toString(), StandardCharsets.UTF_8);
    }
}
