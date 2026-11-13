package raft04;
 
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
 
/**
 * Uso: RaftCheck <N> <pasta> [falhas.txt]
 *
 * Lê no-*, estado-*, hb-*, eventos-*, truncados-*, monitor-* e produtor.txt de uma pasta e
 * escreve o relatório (consola + raftcheck-relatorio.txt na pasta):
 *   1. sequência de termos por partição (candidatos, votos, líder / voto dividido, duração);
 *   2. segurança da eleição (termos com mais de um líder, termo nunca diminui, um voto por termo);
 *   3. tempo até ao primeiro líder;
 *   4. indisponibilidade de cada falha (falhas.txt: p<k>;n<i>;instante) e leituras nesse intervalo;
 *   5. leituras perdidas;
 *   6. registos truncados e quantos existem, com outro seq, no registo final do líder;
 *   7. coincidência final dos ficheiros dos nós.
 *
 * Eventos usados (formato das linhas de eventos-p<k>-n<i>.txt: instante;EVENTO;p<k>;detalhe):
 *   ARRANQUE, CANDIDATO, VOTO, LIDER, NOVO_LIDER, TERMO, DEMISSAO, RECEBIDO, IGNORADO, TRUNCADO.
 */
public class RaftCheck {
 
    private static final StringBuilder OUT = new StringBuilder();
 
    private static void p(String s) {
        System.out.println(s);
        OUT.append(s).append(System.lineSeparator());
    }
 
    // ------------------------------------------------------------------ modelo
 
    static final class Ev {
        final long t;
        final String tipo;
        final int node;
        final String det;
 
        Ev(long t, String tipo, int node, String det) {
            this.t = t;
            this.tipo = tipo;
            this.node = node;
            this.det = det;
        }
 
        String kv(String key) {
            for (String part : det.split(";")) {
                part = part.trim();
                if (part.startsWith(key + "=")) {
                    return part.substring(key.length() + 1);
                }
            }
            return null;
        }
 
        long lkv(String key) {
            String v = kv(key);
            if (v == null) return -1;
            try {
                return Long.parseLong(v.trim());
            } catch (NumberFormatException e) {
                return -1;
            }
        }
    }
 
    static final class Prod {
        final long t;
        final int k;
        final String sensor;
        final double temp;
 
        Prod(long t, int k, String sensor, double temp) {
            this.t = t;
            this.k = k;
            this.sensor = sensor;
            this.temp = temp;
        }
 
        String key() { return sensor + "|" + temp; }
        String show() { return sensor + " " + temp + " @" + t; }
    }
 
    static final class Falha {
        final int k;
        final int node;
        final long t;
 
        Falha(int k, int node, long t) {
            this.k = k;
            this.node = node;
            this.t = t;
        }
    }
 
    static final class Janela {
        final int k;
        final long ini;
        final long fim;   // Long.MAX_VALUE se não houve novo líder
 
        Janela(int k, long ini, long fim) {
            this.k = k;
            this.ini = ini;
            this.fim = fim;
        }
    }
 
    static final class Term {
        final long t;
        final Map<Integer, Long> cand = new TreeMap<>();               // nó -> instante do CANDIDATO
        final Map<Integer, Set<Integer>> grants = new TreeMap<>();     // candidato -> eleitores que concederam
        final Set<Integer> leaders = new TreeSet<>();
        long leaderT = -1;
 
        Term(long t) { this.t = t; }
    }
 
    static final class Part {
        final int k;
        final List<Ev> evs = new ArrayList<>();
        final Map<Integer, List<SensorRecord>> recs = new TreeMap<>();
        final Map<Integer, Map<Long, String>> raw = new TreeMap<>();
        final List<long[]> hbs = new ArrayList<>();                    // {termo, hb, instante, nó}
        final Map<Integer, Long> estadoTermo = new TreeMap<>();
        final Map<Integer, List<List<SensorRecord>>> blocos = new TreeMap<>();
        int avisos = 0;
 
        Part(int k) { this.k = k; }
    }
 
    // ------------------------------------------------------------------ leitura
 
    static List<String> lines(Path f) throws IOException {
        List<String> out = new ArrayList<>();
        if (!Files.exists(f)) return out;
        for (String l : Files.readAllLines(f, StandardCharsets.UTF_8)) {
            if (!l.isBlank()) out.add(l.trim());
        }
        return out;
    }
 
    static Part load(Path dir, int k) throws IOException {
        Part pt = new Part(k);
        for (int i = 1; i <= Wire.GRUPO; i++) {
            String sfx = "p" + k + "-n" + i + ".txt";
 
            for (String l : lines(dir.resolve("eventos-" + sfx))) {
                String[] f = l.split(";", 4);
                if (f.length < 3) continue;
                try {
                    pt.evs.add(new Ev(Long.parseLong(f[0].trim()), f[1].trim(), i, f.length > 3 ? f[3] : ""));
                } catch (NumberFormatException e) {
                    pt.avisos++;
                }
            }
 
            Path no = dir.resolve("no-" + sfx);
            if (Files.exists(no)) {
                List<SensorRecord> rs = new ArrayList<>();
                Map<Long, String> raw = new LinkedHashMap<>();
                for (String l : lines(no)) {
                    try {
                        SensorRecord r = SensorRecord.fromLine(l);
                        rs.add(r);
                        raw.put(r.getSeq(), l);
                    } catch (RuntimeException e) {
                        pt.avisos++;
                    }
                }
                pt.recs.put(i, rs);
                pt.raw.put(i, raw);
            }
 
            for (String l : lines(dir.resolve("hb-" + sfx))) {
                String[] f = l.split(";", -1);
                try {
                    pt.hbs.add(new long[]{Long.parseLong(f[0].trim()), Long.parseLong(f[1].trim()),
                            Long.parseLong(f[2].trim()), i});
                } catch (RuntimeException e) {
                    pt.avisos++;
                }
            }
 
            Path es = dir.resolve("estado-" + sfx);
            if (Files.exists(es)) {
                try {
                    pt.estadoTermo.put(i, Long.parseLong(Files.readString(es, StandardCharsets.UTF_8)
                            .trim().split(";")[0]));
                } catch (RuntimeException e) {
                    pt.avisos++;
                }
            }
 
            List<List<SensorRecord>> blocos = new ArrayList<>();
            for (String l : lines(dir.resolve("truncados-" + sfx))) {
                if (l.startsWith("#TRUNCADO")) {
                    blocos.add(new ArrayList<>());
                } else if (!blocos.isEmpty()) {
                    try {
                        blocos.get(blocos.size() - 1).add(SensorRecord.fromLine(l));
                    } catch (RuntimeException e) {
                        pt.avisos++;
                    }
                }
            }
            if (!blocos.isEmpty()) pt.blocos.put(i, blocos);
        }
        pt.evs.sort(Comparator.comparingLong((Ev e) -> e.t));          // estável: mantém a ordem de cada ficheiro
        return pt;
    }
 
    static List<Prod> readProd(Path dir) throws IOException {
        List<Prod> out = new ArrayList<>();
        for (String l : lines(dir.resolve("produtor.txt"))) {
            String[] f = l.split(";", -1);
            try {
                out.add(new Prod(Long.parseLong(f[0].trim()), Integer.parseInt(f[1].trim().substring(1)),
                        f[2].trim(), Double.parseDouble(f[3].trim())));
            } catch (RuntimeException e) {
                System.err.println("produtor.txt: linha ignorada: " + l);
            }
        }
        out.sort(Comparator.comparingLong((Prod x) -> x.t));
        return out;
    }
 
    static List<Falha> readFalhas(Path f) throws IOException {
        List<Falha> out = new ArrayList<>();
        for (String l : lines(f)) {
            if (l.startsWith("#")) continue;
            String[] t = l.split(";", -1);
            try {
                out.add(new Falha(Integer.parseInt(t[0].trim().substring(1)),
                        Integer.parseInt(t[1].trim().substring(1)), Long.parseLong(t[2].trim())));
            } catch (RuntimeException e) {
                System.err.println("falhas: linha ignorada: " + l);
            }
        }
        out.sort(Comparator.comparingLong((Falha x) -> x.t));
        return out;
    }
 
    static int nodeNum(String s) {
        s = s.trim();
        return Integer.parseInt(s.startsWith("n") ? s.substring(1) : s);
    }
 
    static Term term(TreeMap<Long, Term> m, long t) {
        return m.computeIfAbsent(t, Term::new);
    }
 
    // ------------------------------------------------------------------ main
 
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Uso: RaftCheck <N> <pasta> [falhas.txt]");
            return;
        }
        int n = Integer.parseInt(args[0]);
        Path dir = Paths.get(args[1]);
        List<Falha> falhas = new ArrayList<>();
        if (args.length > 2) {
            Path f = Paths.get(args[2]);
            if (!Files.exists(f)) f = dir.resolve(args[2]);
            falhas = readFalhas(f);
        }
 
        p("RaftCheck | N=" + n + " | G=" + Wire.GRUPO + " | pasta=" + dir.toAbsolutePath());
        Part[] parts = new Part[n];
        for (int k = 0; k < n; k++) parts[k] = load(dir, k);
        List<Prod> prod = readProd(dir);
 
        int multiLeaderTotal = 0;
        int violacoesTermo = 0;
        int votosDuplos = 0;
        for (Part pt : parts) {
            p("");
            p("================ Partição p" + pt.k + " ================");
            if (pt.avisos > 0) p("(aviso: " + pt.avisos + " linhas ilegíveis ignoradas)");
            int[] r = eleicoes(pt);
            multiLeaderTotal += r[0];
            violacoesTermo += r[1];
            votosDuplos += r[2];
        }
 
        p("");
        p("================ Indisponibilidade após falhas ================");
        List<Janela> janelas = new ArrayList<>();
        if (falhas.isEmpty()) {
            p("(sem falhas.txt: nada a medir)");
        } else {
            for (Falha f : falhas) indisponibilidade(parts, prod, f, janelas);
        }
 
        p("");
        p("================ Leituras perdidas ================");
        int perdidasTotal = leiturasPerdidas(parts, prod, janelas);
 
        p("");
        p("================ Registos truncados ================");
        int truncadosTotal = truncagens(parts);
 
        p("");
        p("================ Coincidência final dos ficheiros ================");
        int divergentes = coincidencia(parts);
 
        p("");
        p("================ Detetores (monitor-*.txt) ================");
        detetores(dir);
 
        p("");
        p("================ RESUMO ================");
        p("termos com mais de um líder ........ " + multiLeaderTotal + (multiLeaderTotal == 0 ? "  (OK)" : "  (FALHA)"));
        p("diminuições do termo num nó ........ " + violacoesTermo + (violacoesTermo == 0 ? "  (OK)" : "  (FALHA)"));
        p("nós com 2 votos no mesmo termo ..... " + votosDuplos + (votosDuplos == 0 ? "  (OK)" : "  (FALHA)"));
        p("leituras perdidas .................. " + perdidasTotal);
        p("registos truncados ................. " + truncadosTotal);
        p("seq divergentes na coincidência .... " + divergentes + (divergentes == 0 ? "  (OK)" : "  (FALHA)"));
 
        Files.writeString(dir.resolve("raftcheck-relatorio.txt"), OUT.toString(), StandardCharsets.UTF_8);
    }
 
    // ------------------------------------------------------------------ 1-3: eleições, segurança, primeiro líder
 
    /** Devolve {termos com mais de um líder, diminuições de termo, nós com voto duplo}. */
    static int[] eleicoes(Part pt) {
        TreeMap<Long, Term> terms = new TreeMap<>();
        for (Ev e : pt.evs) {
            switch (e.tipo) {
                case "CANDIDATO": {
                    long t = e.lkv("termo");
                    if (t >= 0) term(terms, t).cand.putIfAbsent(e.node, e.t);
                    break;
                }
                case "VOTO": {
                    long t = e.lkv("termo");
                    String c = e.kv("candidato");
                    if (t >= 0 && c != null && e.det.contains("concedido")) {
                        term(terms, t).grants.computeIfAbsent(nodeNum(c), x -> new TreeSet<>()).add(e.node);
                    }
                    break;
                }
                case "LIDER": {
                    long t = e.lkv("termo");
                    if (t >= 0) {
                        Term T = term(terms, t);
                        T.leaders.add(e.node);
                        if (T.leaderT < 0) T.leaderT = e.t;
                    }
                    break;
                }
                default:
                    break;
            }
        }
 
        p("-- Sequência de termos --");
        int divididos = 0;
        if (terms.isEmpty()) p("(sem eventos de eleição)");
        for (Term T : terms.values()) {
            Set<Integer> candidatos = new TreeSet<>(T.cand.keySet());
            candidatos.addAll(T.grants.keySet());
            StringBuilder votos = new StringBuilder();
            for (int c : candidatos) {
                int v = T.grants.getOrDefault(c, new TreeSet<>()).size() + (T.cand.containsKey(c) ? 1 : 0);
                if (votos.length() > 0) votos.append(", ");
                votos.append("n").append(c).append("=").append(v);
            }
            String res;
            if (!T.leaders.isEmpty()) {
                long ini = T.cand.isEmpty() ? T.leaderT : java.util.Collections.min(T.cand.values());
                res = "líder " + T.leaders.stream().map(x -> "n" + x).collect(Collectors.toList())
                        + " | eleição " + (T.leaderT - ini) + " ms";
            } else if (T.cand.size() >= 2) {
                res = "voto dividido";
                divididos++;
            } else {
                res = "sem líder (candidato único sem maioria)";
            }
            p("termo " + T.t + ": candidatos " + candidatos.stream().map(x -> "n" + x).collect(Collectors.toList())
                    + " | votos " + votos + " | " + res);
        }
        p("termos com voto dividido: " + divididos);
 
        // -- segurança --
        p("-- Segurança --");
        int multi = 0;
        for (Term T : terms.values()) {
            if (T.leaders.size() > 1) {
                multi++;
                p("FALHA: termo " + T.t + " com " + T.leaders.size() + " líderes " + T.leaders);
            }
        }
        p("termos com mais de um líder: " + multi + (multi == 0 ? " (OK)" : " (FALHA)"));
 
        int viol = 0;
        Map<Integer, Long> prev = new HashMap<>();
        Map<Integer, Long> maxVisto = new HashMap<>();
        for (Ev e : pt.evs) {
            long[] vs;
            switch (e.tipo) {
                case "ARRANQUE": case "CANDIDATO": case "VOTO": case "LIDER": case "NOVO_LIDER":
                case "DEMISSAO": case "RECEBIDO":
                    vs = new long[]{e.lkv("termo")};
                    break;
                case "IGNORADO":
                    vs = new long[]{e.lkv("termoAtual")};
                    break;
                case "TERMO": {
                    String[] ab = e.det.split("->");
                    try {
                        long a = Long.parseLong(ab[0].trim());
                        long b = Long.parseLong(ab[1].trim());
                        if (b <= a) {
                            viol++;
                            p("FALHA: n" + e.node + " @" + e.t + " TERMO " + a + "->" + b + " (não aumenta)");
                        }
                        vs = new long[]{a, b};
                    } catch (RuntimeException ex) {
                        vs = new long[0];
                    }
                    break;
                }
                default:
                    vs = new long[0];
            }
            for (long v : vs) {
                if (v < 0) continue;
                Long pv = prev.get(e.node);
                if (pv != null && v < pv) {
                    viol++;
                    p("FALHA: n" + e.node + " @" + e.t + " termo desceu de " + pv + " para " + v + " (" + e.tipo + ")");
                }
                prev.put(e.node, v);
                maxVisto.merge(e.node, v, Math::max);
            }
        }
        for (Map.Entry<Integer, Long> en : pt.estadoTermo.entrySet()) {
            Long mv = maxVisto.get(en.getKey());
            if (mv != null && en.getValue() < mv) {
                viol++;
                p("FALHA: estado-p" + pt.k + "-n" + en.getKey() + ".txt tem termo " + en.getValue()
                        + " < " + mv + " visto nos eventos (estado não persistido)");
            }
        }
        p("diminuições do termo: " + viol + (viol == 0 ? " (OK)" : " (FALHA)"));
 
        // um voto por termo e por nó (o voto em si próprio, ao candidatar-se, também conta)
        int duplos = 0;
        Map<Integer, Map<Long, Set<Integer>>> votou = new TreeMap<>();
        for (Ev e : pt.evs) {
            if (e.tipo.equals("CANDIDATO") && e.lkv("termo") >= 0) {
                votou.computeIfAbsent(e.node, x -> new TreeMap<>())
                        .computeIfAbsent(e.lkv("termo"), x -> new TreeSet<>()).add(e.node);
            } else if (e.tipo.equals("VOTO") && e.det.contains("concedido") && e.kv("candidato") != null) {
                votou.computeIfAbsent(e.node, x -> new TreeMap<>())
                        .computeIfAbsent(e.lkv("termo"), x -> new TreeSet<>()).add(nodeNum(e.kv("candidato")));
            }
        }
        for (Map.Entry<Integer, Map<Long, Set<Integer>>> en : votou.entrySet()) {
            for (Map.Entry<Long, Set<Integer>> t : en.getValue().entrySet()) {
                if (t.getValue().size() > 1) {
                    duplos++;
                    p("FALHA: n" + en.getKey() + " votou em " + t.getValue() + " no termo " + t.getKey());
                }
            }
        }
        p("nós com mais de um voto no mesmo termo: " + duplos + (duplos == 0 ? " (OK)" : " (FALHA)"));
 
        // -- primeiro líder --
        p("-- Tempo até ao primeiro líder --");
        long arr = Long.MAX_VALUE;
        for (Ev e : pt.evs) if (e.tipo.equals("ARRANQUE")) arr = Math.min(arr, e.t);
        boolean aprox = false;
        if (arr == Long.MAX_VALUE && !pt.evs.isEmpty()) {
            arr = pt.evs.get(0).t;
            aprox = true;
        }
        long primeiro = Long.MAX_VALUE;
        long termoPrimeiro = -1;
        for (Ev e : pt.evs) {
            if (e.tipo.equals("LIDER")) {
                primeiro = e.t;
                termoPrimeiro = e.lkv("termo");
                break;
            }
        }
        if (primeiro == Long.MAX_VALUE || arr == Long.MAX_VALUE) {
            p("a partição nunca teve líder nos ficheiros analisados");
        } else {
            p((primeiro - arr) + " ms desde o arranque do primeiro nó" + (aprox
                    ? " (APROXIMADO: sem evento ARRANQUE; conta desde o primeiro evento registado)" : "")
                    + " | termos até ao primeiro líder: " + termoPrimeiro);
        }
        return new int[]{multi, viol, duplos};
    }
 
    // ------------------------------------------------------------------ 4: indisponibilidade
 
    static void indisponibilidade(Part[] parts, List<Prod> prod, Falha f, List<Janela> janelas) {
        if (f.k < 0 || f.k >= parts.length) {
            p("falha ignorada (partição inexistente): p" + f.k);
            return;
        }
        Part pt = parts[f.k];
        Ev lider = null;
        for (Ev e : pt.evs) {
            if (e.tipo.equals("LIDER") && e.t <= f.t) lider = e;
        }
        String id = "falha p" + f.k + ";n" + f.node + " @" + f.t;
        if (lider == null) {
            p(id + ": nenhum líder conhecido antes da falha");
            return;
        }
        long T = lider.lkv("termo");
        if (lider.node != f.node) {
            p(id + ": n" + f.node + " era seguidor (líder: n" + lider.node + ", termo " + T
                    + "); sem indisponibilidade esperada");
            return;
        }
        long melhor = Long.MAX_VALUE;
        int novoNo = -1;
        long novoTermo = -1;
        for (long[] h : pt.hbs) {
            if (h[0] > T && h[2] >= f.t && h[2] < melhor) {
                melhor = h[2];
                novoNo = (int) h[3];
                novoTermo = h[0];
            }
        }
        long fim = melhor;
        long ate = fim == Long.MAX_VALUE ? Long.MAX_VALUE : fim;
        List<Prod> lidas = new ArrayList<>();
        for (Prod x : prod) {
            if (x.k == f.k && x.t >= f.t && x.t < ate) lidas.add(x);
        }
        janelas.add(new Janela(f.k, f.t, fim));
        if (fim == Long.MAX_VALUE) {
            p(id + ": líder do termo " + T + " falhou e não houve LOTE de termo superior nos ficheiros"
                    + " | leituras enviadas desde a falha: " + lidas.size());
        } else {
            p(id + ": líder do termo " + T + " falhou | novo líder n" + novoNo + " (termo " + novoTermo
                    + ") com 1.º LOTE @" + fim + " | indisponibilidade " + (fim - f.t) + " ms ("
                    + String.format("%.1f", (fim - f.t) / 1000.0) + " s) | leituras enviadas no intervalo: "
                    + lidas.size());
        }
        for (Prod x : lidas) p("    " + x.show());
    }
 
    // ------------------------------------------------------------------ 5: leituras perdidas
 
    static int leiturasPerdidas(Part[] parts, List<Prod> prod, List<Janela> janelas) {
        int total = 0;
        for (Part pt : parts) {
            // disponíveis: para cada (sensor, temperatura), o máximo, entre os nós, de ocorrências no registo final
            Map<String, Integer> disp = new HashMap<>();
            for (List<SensorRecord> rs : pt.recs.values()) {
                Map<String, Integer> c = new HashMap<>();
                for (SensorRecord r : rs) c.merge(r.getSensorId() + "|" + r.getTemperature(), 1, Integer::sum);
                for (Map.Entry<String, Integer> en : c.entrySet()) disp.merge(en.getKey(), en.getValue(), Math::max);
            }
            Map<String, Integer> usado = new HashMap<>();
            int enviadas = 0;
            int duranteEleicao = 0;
            List<Prod> perdidas = new ArrayList<>();
            for (Prod x : prod) {
                if (x.k != pt.k) continue;
                enviadas++;
                int u = usado.getOrDefault(x.key(), 0);
                if (u < disp.getOrDefault(x.key(), 0)) {
                    usado.put(x.key(), u + 1);
                } else {
                    perdidas.add(x);
                }
            }
            for (Prod x : perdidas) {
                for (Janela j : janelas) {
                    if (j.k == pt.k && x.t >= j.ini && x.t < j.fim) {
                        duranteEleicao++;
                        break;
                    }
                }
            }
            p("p" + pt.k + ": enviadas " + enviadas + " | perdidas " + perdidas.size()
                    + " (na janela sem líder: " + duranteEleicao + "; fora dela, p.ex. truncagem: "
                    + (perdidas.size() - duranteEleicao) + ")");
            for (Prod x : perdidas) p("    perdida: " + x.show());
            total += perdidas.size();
        }
        p("(correspondência por sensor+temperatura, na ordem do envio; leituras repetidas com os mesmos valores"
                + " podem atribuir a perda à ocorrência mais recente)");
        return total;
    }
 
    // ------------------------------------------------------------------ 6: truncagens
 
    static int truncagens(Part[] parts) {
        int total = 0;
        for (Part pt : parts) {
            // registo final do líder: nó do LIDER de maior termo
            long maxT = -1;
            int lider = -1;
            for (Ev e : pt.evs) {
                if (e.tipo.equals("LIDER") && e.lkv("termo") >= maxT) {
                    maxT = e.lkv("termo");
                    lider = e.node;
                }
            }
            List<SensorRecord> finalLider = lider > 0 && pt.recs.containsKey(lider)
                    ? pt.recs.get(lider) : new ArrayList<>();
            Map<Integer, Integer> idx = new HashMap<>();
            int desta = 0;
            for (Ev e : pt.evs) {
                if (!e.tipo.equals("TRUNCADO")) continue;
                int i = idx.merge(e.node, 1, Integer::sum) - 1;
                long desc = e.lkv("descartados");
                long de = e.lkv("de");
                total += (int) Math.max(desc, 0);
                desta++;
                String existem;
                List<List<SensorRecord>> bl = pt.blocos.get(e.node);
                if (bl != null && i < bl.size()) {
                    boolean[] usado = new boolean[finalLider.size()];
                    int n = 0;
                    for (SensorRecord d : bl.get(i)) {
                        for (int j = 0; j < finalLider.size(); j++) {
                            SensorRecord r = finalLider.get(j);
                            if (!usado[j] && r.getSeq() != d.getSeq() && r.getSensorId().equals(d.getSensorId())
                                    && Double.compare(r.getTemperature(), d.getTemperature()) == 0) {
                                usado[j] = true;
                                n++;
                                break;
                            }
                        }
                    }
                    existem = n + " de " + bl.get(i).size();
                } else {
                    existem = "n/d (sem truncados-p" + pt.k + "-n" + e.node + ".txt para este evento)";
                }
                p("p" + pt.k + " n" + e.node + " @" + e.t + ": TRUNCADO de=" + de + " descartados=" + desc
                        + " termos=" + e.kv("termos") + " | existem com outro seq no registo final do líder"
                        + (lider > 0 ? " n" + lider : "") + ": " + existem);
            }
            if (desta == 0) p("p" + pt.k + ": nenhum TRUNCADO");
        }
        p("total de registos descartados: " + total);
        return total;
    }
 
    // ------------------------------------------------------------------ 7: coincidência final
 
    static int coincidencia(Part[] parts) {
        int totalDiv = 0;
        for (Part pt : parts) {
            List<Integer> nos = new ArrayList<>(pt.raw.keySet());
            StringBuilder sb = new StringBuilder();
            long menor = Long.MAX_VALUE;
            for (int no : nos) {
                List<SensorRecord> rs = pt.recs.get(no);
                long last = rs.isEmpty() ? 0 : rs.get(rs.size() - 1).getSeq();
                sb.append(" n").append(no).append("=").append(last);
                menor = Math.min(menor, last);
            }
            if (nos.size() < 2) {
                p("p" + pt.k + ": menos de dois ficheiros no-p" + pt.k + "-n*.txt (" + nos + "); nada a comparar");
                continue;
            }
            List<Long> diff = new ArrayList<>();
            for (long s = 1; s <= menor; s++) {
                String ref = null;
                boolean difere = false;
                for (int no : nos) {
                    String l = pt.raw.get(no).get(s);
                    if (l == null) {
                        difere = true;
                        break;
                    }
                    if (ref == null) ref = l;
                    else if (!ref.equals(l)) {
                        difere = true;
                        break;
                    }
                }
                if (difere) diff.add(s);
            }
            p("p" + pt.k + ": ultimoSeq" + sb + " | comparados 1.." + menor + " | seq divergentes: " + diff.size()
                    + (diff.isEmpty() ? " (OK)" : " " + diff));
            totalDiv += diff.size();
        }
        return totalDiv;
    }
 
    // ------------------------------------------------------------------ monitores
 
    static void detetores(Path dir) throws IOException {
        List<Path> ficheiros = new ArrayList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir, "monitor-*.txt")) {
            for (Path f : ds) ficheiros.add(f);
        }
        ficheiros.sort(Comparator.comparing(Path::toString));
        if (ficheiros.isEmpty()) p("(sem ficheiros monitor-*.txt)");
        for (Path f : ficheiros) {
            Map<String, Integer> c = new TreeMap<>();
            for (String l : lines(f)) {
                String[] t = l.split(";", 4);
                if (t.length >= 3 && !t[1].equals("RECEBIDO")) c.merge(t[1] + " " + t[2], 1, Integer::sum);
            }
            p(f.getFileName() + ": " + (c.isEmpty() ? "(sem eventos além de RECEBIDO)" : c.toString()));
        }
    }
}
