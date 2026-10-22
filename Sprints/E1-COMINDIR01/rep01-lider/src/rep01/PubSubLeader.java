package rep01;

import java.io.*;
import java.util.Scanner;

public class PubSubLeader {

    public static void main(String[] args) {
        String API = args.length > 0 ? args[0] : "http://127.0.0.1:5001/api/v0";
        try(
            Scanner sc = new Scanner(System.in)) {
            RecordFile file = new RecordFile("lider.txt");
            long seq = file.lastSeq(); // continua a numeração após reinício
            System.out.println("Líder pronto. Formato: <sensor> <temperatura> | 'sair' para terminar");

            while (true) {
                IpfsPubSub pubSub = new IpfsPubSub(API);
                
                System.out.print("> ");
                if (!sc.hasNextLine())
                    break; // bloqueia à espera do utilizador
                String line = sc.nextLine().trim();
                if (line.equalsIgnoreCase("sair"))
                    break;
                String[] p = line.split("\\s+");
                if (p.length != 2) {
                    System.out.println("Formato inválido.");
                    continue;
                }
                
                if (p[0].equalsIgnoreCase("rajada") && p.length == 2) {
                    int n;

                    try { 
                        n = Integer.parseInt(p[1]); 
                    }catch (NumberFormatException e) { 
                        System.out.println("n inválido"); 
                        continue; 
                    }
                    
                    long primeiro = 0, ultimo = 0;

                    for (int i = 0; i < n; i++) {
                        double temp = Math.round((15 + Math.random() * 15) * 10) / 10.0;
                        SensorRecord r = SensorRecord.now(++seq, "SIM", temp);
                        file.append(r);
                        String m = r.toLine();
                        if (i == 0) primeiro = System.currentTimeMillis();
                        try{
                            pubSub.publish("rep01-temperaturas", m);
                        }catch(InterruptedException e){
                            System.out.println("IE: " + e.getMessage());
                            Thread.currentThread().interrupt();
                            return;
                        }
                        
                        ultimo = System.currentTimeMillis();
                    }

                    System.out.println("Rajada: 1.º=" + primeiro + " último=" + ultimo
                            + " (" + (ultimo - primeiro) + " ms)");
                    continue;
                }

                double temp;
                try {
                    temp = Double.parseDouble(p[1]);
                } catch (NumberFormatException e) {
                    System.out.println("Temperatura inválida: " + p[1]);
                    continue;
                }

                SensorRecord r = SensorRecord.now(++seq, p[0], temp);
                file.append(r); // 1) escreve localmente
                String m = r.toLine();

                
                try{
                    pubSub.publish("rep01-temperaturas", m);
                }catch(InterruptedException e){
                    System.out.println("IE: " + e.getMessage());
                    Thread.currentThread().interrupt();
                    return;
                }
                System.out.println("Registado e enviado: " + r.toLine());
            }
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }
}