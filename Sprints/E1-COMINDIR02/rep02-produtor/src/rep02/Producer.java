package rep02;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Random;

/**
 * Uso: Producer <N> [api]
 * Comandos:
 *   <sensor> <temperatura>
 *   rajada <n> <s>
 *   rajada-quente <n> <s>
 *   sair
 *
 * Assume que IpfsPubSub (COMINDIR01) tem: publish(String topico, String msg).
 */
public class Producer {

    private static final Random RND = new Random();

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Uso: Producer <N> [api]");
            return;
        }
        int n = Integer.parseInt(args[0]);
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";

        Partitioner part = new Partitioner(n);
        IpfsPubSub ps = new IpfsPubSub(api);

        System.out.println("Produtor pronto (N=" + n + "). Comandos: <sensor> <temp> | rajada <n> <s> | rajada-quente <n> <s> | sair");
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        String linha;
        while ((linha = in.readLine()) != null) {
            linha = linha.trim();
            if (linha.isEmpty()) {
                continue;
            }
            if (linha.equalsIgnoreCase("sair")) {
                break;
            }
            String[] t = linha.split("\\s+");
            try {
                if (t[0].equals("rajada") && t.length == 3) {
                    rajada(ps, part, Integer.parseInt(t[1]), Integer.parseInt(t[2]), false);
                } else if (t[0].equals("rajada-quente") && t.length == 3) {
                    rajada(ps, part, Integer.parseInt(t[1]), Integer.parseInt(t[2]), true);
                } else if (t.length == 2) {
                    double temp = Double.parseDouble(t[1].replace(',', '.'));
                    enviar(ps, part, t[0], temp);
                } else {
                    System.out.println("Comando inválido: " + linha);
                }
            } catch (NumberFormatException e) {
                System.out.println("Número inválido: " + linha);
            } catch (Exception e) {
                System.out.println("Falha a publicar: " + e);
            }
        }
    }

    private static int enviar(IpfsPubSub ps, Partitioner part, String sensor, double temp) throws Exception {
        int k = part.of(sensor);
        ps.publish(Wire.topicIn(k), Wire.in(sensor, temp));
        return k;
    }

    private static void rajada(IpfsPubSub ps, Partitioner part, int total, int s, boolean quente) throws Exception {
        int[] porParticao = new int[part.count()];
        long primeira = 0;
        long ultima = 0;
        for (int i = 0; i < total; i++) {
            String sensor = escolherSensor(s, quente);
            double temp = Math.round((15 + RND.nextDouble() * 15) * 10) / 10.0;
            long agora = System.currentTimeMillis();
            if (i == 0) {
                primeira = agora;
            }
            int k = enviar(ps, part, sensor, temp);
            ultima = System.currentTimeMillis();
            porParticao[k]++;
        }
        System.out.println("Rajada terminada: " + total + " leituras");
        System.out.println("  primeira publicação: " + primeira + " ms; última: " + ultima + " ms; duração: " + (ultima - primeira) + " ms");
        for (int k = 0; k < porParticao.length; k++) {
            System.out.println("  p" + k + ": " + porParticao[k] + " leituras");
        }
    }

    private static String escolherSensor(int s, boolean quente) {
        if (quente) {
            if (s < 2 || RND.nextDouble() < 0.8) {
                return "S01";
            }
            return String.format("S%02d", 2 + RND.nextInt(s - 1));   // S02..S<s>
        }
        return String.format("S%02d", 1 + RND.nextInt(s));           // S01..S<s>
    }
}
