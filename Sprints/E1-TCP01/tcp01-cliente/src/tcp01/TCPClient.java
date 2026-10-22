package tcp01;

import java.io.*;
import java.net.*;


public class TCPClient {
    public static void main(String[] args) {
        Socket s = null;
        try {
            int serverPort = 7896; // port do servidor
            s = new Socket("localhost", serverPort); // bloqueia espera que a ligação ao servidor seja estabelecida
            DataInputStream in = new DataInputStream(s.getInputStream());
            ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
            Place place = new Place("3500-000", "Viseu");
            Person p = new Person("Ana", place, 2000);
            out.writeObject(p);
            out.flush(); // envia os dados ao servidor
            String data = in.readUTF(); // bloqueia a espera da resposta
            System.out.println("Received: " + data);
        } catch (UnknownHostException e) {
            System.out.println("Sock: " + e.getMessage());
        } catch (EOFException e) {
            System.out.println("EOF: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        } finally {
            if (s != null) {
                try {
                    s.close();
                } catch (IOException e) {
                    System.out.println("close: " + e.getMessage());
                }
            }
        }
    }
}