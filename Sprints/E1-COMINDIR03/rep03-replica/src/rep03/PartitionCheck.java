package rep03;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Uso: PartitionCheck <N> <pasta> [R]      (R por omissão = 2)
 * Lê lider-p*.txt e replica-*-p*.txt da pasta e verifica cada partição.
 * Não depende de SensorRecord: lê as linhas "seq;sensor;temperatura;instante" diretamente.
 */
public class PartitionCheck {

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Uso: PartitionCheck <N> <pasta> [R]");
            return;
        }
        int n = Integer.parseInt(args[0]);
        Path pasta = Paths.get(args[1]);
        int rEsperado = args.length > 2 ? Integer.parseInt(args[2]) : 2;
        Partitioner part = new Partitioner(n);

        Pattern pl = Pattern.compile("lider-p(\\d+)\\.txt");
        Pattern pr = Pattern.compile("replica-(\\d+)-p(\\d+)\\.txt");

        Map<Integer, List<String>> lideres = new TreeMap<>();
        Map<Integer, Map<Integer, List<String>>> replicas = new TreeMap<>();   // k -> (id -> linhas)

        try (DirectoryStream<Path> ds = Files.newDirectoryStream(pasta)) {
            for (Path f : ds) {
                String nome = f.getFileName().toString();
                Matcher ml = pl.matcher(nome);
                Matcher mr = pr.matcher(nome);
                if (ml.matches()) {
                    lideres.put(Integer.parseInt(ml.group(1)), ler(f));
                } else if (mr.matches()) {
                    int rid = Integer.parseInt(mr.group(1));
                    int k = Integer.parseInt(mr.group(2));
                    replicas.computeIfAbsent(k, x -> new TreeMap<>()).put(rid, ler(f));
                }
            }
        }

        int problemas = 0;
        long somaParticoes = 0;

        for (int k = 0; k < n; k++) {
            System.out.println("=== Partição " + k + " ===");
            List<String> lider = lideres.get(k);
            if (lider == null) {
                System.out.println("  ERRO: falta lider-p" + k + ".txt");
                problemas++;
                continue;
            }
            somaParticoes += lider.size();

            Set<String> sensores = new HashSet<>();
            int errados = 0;
            for (String linha : lider) {
                String[] f = linha.split(";");
                if (f.length < 2) {
                    errados++;
                    continue;
                }
                sensores.add(f[1]);
                if (part.of(f[1]) != k) {
                    errados++;
                }
            }
            System.out.println("  líder: " + lider.size() + " registos, " + sensores.size() + " sensores distintos");
            if (errados > 0) {
                System.out.println("  ERRO: " + errados + " registos de sensores que não pertencem a p" + k);
                problemas++;
            } else {
                System.out.println("  OK: todos os sensores pertencem a p" + k);
            }
            String sq = verificarSeq(lider);
            if (sq != null) {
                System.out.println("  ERRO sequência do líder: " + sq);
                problemas++;
            } else {
                System.out.println("  OK: seq do líder de 1 a " + lider.size() + " sem saltos nem repetições");
            }

            Map<Integer, List<String>> reps = replicas.getOrDefault(k, new TreeMap<>());
            System.out.println("  réplicas que guardam p" + k + ": " + reps.size()
                    + (reps.size() == rEsperado ? " (OK, R=" + rEsperado + ")" : " (ERRO, esperado R=" + rEsperado + ")"));
            if (reps.size() != rEsperado) {
                problemas++;
            }
            for (Map.Entry<Integer, List<String>> e : reps.entrySet()) {
                List<String> rep = e.getValue();
                String sr = verificarSeq(rep);
                boolean prefixo = rep.size() <= lider.size() && lider.subList(0, rep.size()).equals(rep);
                String estado;
                if (!prefixo || sr != null) {
                    estado = "DIVERGE" + (sr != null ? " (" + sr + ")" : "");
                    problemas++;
                } else if (rep.size() == lider.size()) {
                    estado = "IGUAL ao líder";
                } else {
                    estado = "PREFIXO do líder (faltam " + (lider.size() - rep.size()) + " registos)";
                }
                System.out.println("    réplica " + e.getKey() + ": " + rep.size() + " registos -> " + estado);
            }
        }

        long total = 0;
        for (List<String> l : lideres.values()) {
            total += l.size();
        }
        System.out.println("=== Total ===");
        System.out.println("  registos nos ficheiros dos líderes: " + total);
        System.out.println("  soma dos registos das partições 0.." + (n - 1) + ": " + somaParticoes);
        if (total != somaParticoes) {
            System.out.println("  ERRO: existem ficheiros de líder fora de [0," + n + ")");
            problemas++;
        } else {
            System.out.println("  OK: total igual à soma das partições");
        }
        System.out.println(problemas == 0 ? "RESULTADO: sem problemas" : "RESULTADO: " + problemas + " problema(s)");
    }

    private static List<String> ler(Path f) throws IOException {
        List<String> out = new ArrayList<>();
        for (String l : Files.readAllLines(f, StandardCharsets.UTF_8)) {
            if (!l.isBlank()) {
                out.add(l.trim());
            }
        }
        return out;
    }

    /** null se seq começa em 1 e cresce de 1 em 1; senão, descrição do primeiro problema. */
    private static String verificarSeq(List<String> linhas) {
        long esperado = 1;
        for (String l : linhas) {
            try {
                long seq = Long.parseLong(l.split(";")[0]);
                if (seq != esperado) {
                    return "seq " + seq + " onde se esperava " + esperado;
                }
            } catch (NumberFormatException e) {
                return "linha inválida: " + l;
            }
            esperado++;
        }
        return null;
    }
}
