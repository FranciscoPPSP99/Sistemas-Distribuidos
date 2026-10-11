package tcp01;

import java.io.*;
import java.net.*;

public class TCPServer {
    public static void main(String[] args) {
        try {
            int serverPort = 7896;
            ServerSocket listenSocket = new ServerSocket(serverPort);
            while (true) {
                Socket clientSocket = listenSocket.accept(); // bloqueia espera que um cliente se ligue ao port
                Connection c = new Connection(clientSocket);   
            }
        } catch (IOException e) {
            System.out.println("Listen: " + e.getMessage());
        }
    }
}