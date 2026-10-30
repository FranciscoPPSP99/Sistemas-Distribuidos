package rep03;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

/** Valida os logs de líderes e produz a vista global ordenada por Lamport. */
public final class CausalCheck {
    private CausalCheck() { }

    private static final class Event {
        final int partition;
        final SensorRecord record;
        Event(int partition, SensorRecord record) {
            this.partition = partition;
            this.record = record;
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Uso: CausalCheck <N> <pasta-dos-registos>");
            return;
        }
        int n = Integer.parseInt(args[0]);
        if (n < 1) throw new IllegalArgumentException("N tem de ser >= 1");
        Path folder = Paths.get(args[1]);
        Files.createDirectories(folder);
        Partitioner partitioner = new Partitioner(n);
        List<Event> all = new ArrayList<>();
        int errors = 0;

        for (int k = 0; k < n; k++) {
            Path file = folder.resolve("lider-p" + k + ".txt");
            System.out.println("=== p" + k + " | " + file + " ===");
            if (!Files.exists(file)) {
                System.out.println("ERRO: ficheiro em falta");
                errors++;
                continue;
            }
            List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
            long expectedSeq = 1;
            long previousLamport = -1;
            int beforeCount = errors;
            for (String line : lines) {
                if (line.isBlank()) continue;
                final SensorRecord r;
                try {
                    r = SensorRecord.fromLine(line);
                } catch (RuntimeException ex) {
                    System.out.println("ERRO: registo mal formado: " + line + " (" + ex.getMessage() + ")");
                    errors++;
                    continue;
                }
                VectorClock v = r.getVector();
                if (r.getSeq() != expectedSeq) {
                    System.out.println("ERRO: seq=" + r.getSeq() + ", esperado=" + expectedSeq);
                    errors++;
                }
                expectedSeq++;
                boolean validVector = v.size() == n;
                if (!validVector) {
                    System.out.println("ERRO: seq=" + r.getSeq() + " tem vetor de dimensao " + v.size()
                            + ", esperado=" + n);
                    errors++;
                } else if (v.get(k) != r.getSeq()) {
                    System.out.println("ERRO: seq=" + r.getSeq() + " mas V[" + k + "]=" + v.get(k));
                    errors++;
                }
                if (r.getLamport() <= previousLamport) {
                    System.out.println("ERRO: L nao cresce na particao (seq=" + r.getSeq()
                            + ", anterior=" + previousLamport + ", atual=" + r.getLamport() + ")");
                    errors++;
                }
                previousLamport = r.getLamport();
                int actualPartition = partitioner.of(r.getSensorId());
                if (actualPartition != k) {
                    System.out.println("ERRO: sensor " + r.getSensorId() + " pertence a p" + actualPartition);
                    errors++;
                }
                if (validVector) all.add(new Event(k, r));
            }
            System.out.println("Registos: " + (expectedSeq - 1) + "; "
                    + (errors == beforeCount ? "sem erros locais detetados" : "foram detetados erros"));
        }

        long crossPartitionPairs = 0;
        long causalPairs = 0;
        long concurrentPairs = 0;
        long lamportOrdersConcurrent = 0;
        for (int i = 0; i < all.size(); i++) {
            Event ea = all.get(i);
            SensorRecord a = ea.record;
            for (int j = i + 1; j < all.size(); j++) {
                Event eb = all.get(j);
                // A métrica global da ficha é entre partições diferentes; pares locais
                // já são verificados pela sequência e pela monotonia de L acima.
                if (ea.partition == eb.partition) continue;
                crossPartitionPairs++;
                SensorRecord b = eb.record;
                VectorClock va = a.getVector();
                VectorClock vb = b.getVector();
                boolean aBeforeB = va.happensBefore(vb);
                boolean bBeforeA = vb.happensBefore(va);
                if (aBeforeB) {
                    causalPairs++;
                    if (a.getLamport() >= b.getLamport()) {
                        System.out.println("ERRO: vetores indicam A antes de B, mas L nao: "
                                + label(ea) + " -> " + label(eb));
                        errors++;
                    }
                } else if (bBeforeA) {
                    causalPairs++;
                    if (b.getLamport() >= a.getLamport()) {
                        System.out.println("ERRO: vetores indicam B antes de A, mas L nao: "
                                + label(eb) + " -> " + label(ea));
                        errors++;
                    }
                } else if (va.concurrentWith(vb)) {
                    concurrentPairs++;
                    if (a.getLamport() != b.getLamport()) lamportOrdersConcurrent++;
                } else {
                    System.out.println("ERRO: vetores iguais em eventos distintos de partições diferentes: "
                            + label(ea) + " e " + label(eb));
                    errors++;
                }
            }
        }

        Path view = folder.resolve("vista-lamport.txt");
        writeLamportView(view, all);

        System.out.println("=== Resumo causal (pares inter-partição) ===");
        System.out.println("Registos analisados: " + all.size());
        System.out.println("Pares inter-partição analisados: " + crossPartitionPairs);
        System.out.println("Pares causalmente ordenados: " + causalPairs);
        System.out.println("Pares concorrentes: " + concurrentPairs);
        System.out.println("Pares concorrentes com L diferentes: " + lamportOrdersConcurrent);
        if (concurrentPairs == 0) {
            System.out.println("Proporção de pares concorrentes distinguidos por Lamport: N/A (não há pares concorrentes)");
        } else {
            double proportion = (100.0 * lamportOrdersConcurrent) / concurrentPairs;
            System.out.printf(Locale.ROOT,
                    "Proporção de pares concorrentes distinguidos por Lamport: %.2f%% (%d/%d)%n",
                    proportion, lamportOrdersConcurrent, concurrentPairs);
        }
        System.out.println("Vista ordenada por Lamport escrita em: " + view.toAbsolutePath());
        System.out.println(errors == 0 ? "RESULTADO: sem problemas detetados"
                : "RESULTADO: " + errors + " problema(s)");
        if (errors != 0) System.exit(1);
    }

    private static String label(Event e) {
        return "p" + e.partition + "/" + e.record.getSensorId() + "#" + e.record.getSeq();
    }

    private static void writeLamportView(Path path, List<Event> events) throws IOException {
        List<Event> sorted = new ArrayList<>(events);
        sorted.sort(Comparator.comparingLong((Event e) -> e.record.getLamport())
                .thenComparingInt(e -> e.partition)
                .thenComparingLong(e -> e.record.getSeq()));
        List<String> lines = new ArrayList<>();
        lines.add("# Vista global por relógio de Lamport; em empate: partição e sequência");
        lines.add("L;particao;seq;sensor;temperatura;timestamp;V");
        for (Event e : sorted) {
            SensorRecord r = e.record;
            lines.add(r.getLamport() + ";p" + e.partition + ";" + r.getSeq() + ";"
                    + r.getSensorId() + ";" + r.getTemperature() + ";" + r.getTimestamp()
                    + ";" + r.getVector().toField());
        }
        Files.write(path, lines, StandardCharsets.UTF_8);
    }
}
