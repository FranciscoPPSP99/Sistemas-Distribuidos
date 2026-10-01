import java.net.*;
import java.io.IOException;
import java.util.*;

public class UDPClient {
	public static void main(String[] args) {
        DatagramSocket aSocket = null;
        try {
            aSocket = new DatagramSocket();
            InetAddress aHost = InetAddress.getByName("localhost");
            int serverPort = 6789;
            try (Scanner scan = new Scanner(System.in)) {
                System.out.println("1 - Modo Automático \n2 - Modo Manual");
                int opc = scan.nextInt();
                if (opc == 1) {
                    ModoAutomatico(aSocket, aHost, serverPort, scan);
                } else if (opc == 2) {
                    ModoManual(aSocket, aHost, serverPort, scan);
                }
                else {
                    System.out.println("Opção inválida.");
                }
            }
         
        }
        catch (SocketException e) {
            System.out.println("Socket: " + e.getMessage());
        }
        catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        } finally {
            if (aSocket != null) aSocket.close();
        }
    }

    private static void ModoAutomatico(DatagramSocket aSocket, InetAddress aHost, int serverPort, Scanner scanner) {
        scanner.nextLine();
        System.out.println("Modo Automático selecionado.");
        String mensagem = "";
        int numeroMensagem = 1;
            while (!mensagem.equals("sair")) {
                System.out.println("Digite a mensagem (ou 'sair' para encerrar):");
                mensagem = scanner.nextLine();
                if (!mensagem.equals("sair")) {
                    mensagem = numeroMensagem + "," + mensagem;
                    enviarMensagem(mensagem, aSocket, aHost, serverPort);
                    numeroMensagem++;
                }
            }
    }

    private static void ModoManual(DatagramSocket aSocket, InetAddress aHost, int serverPort, Scanner scanner) {
        scanner.nextLine();
        System.out.println("Modo Manual selecionado.");
        System.out.println("Quantidade de mensagens a enviar:");
        int quantidadeMensagens = scanner.nextInt();
        scanner.nextLine();
        for (int i = 1; i <= quantidadeMensagens; i++) {
            System.out.println("Digite o número da mensagem: ");
            int numeroMensagem = scanner.nextInt();
            System.out.println("Digite a mensagem: ");
            scanner.nextLine(); 
            String mensagem = numeroMensagem + "," + scanner.nextLine();
            enviarMensagem(mensagem, aSocket, aHost, serverPort);
        }

    }

    private static void enviarMensagem(String mensagem, DatagramSocket aSocket, InetAddress aHost, int serverPort) {
        byte[] messageBytes = mensagem.getBytes();
        DatagramPacket request = new DatagramPacket(messageBytes, messageBytes.length, aHost, serverPort);
        try {
            aSocket.send(request);
            byte[] buffer = new byte[1000];
            DatagramPacket reply = new DatagramPacket(buffer, buffer.length);
            aSocket.receive(reply);
            if (new String(reply.getData(), 0, reply.getLength()).startsWith("waitingfor,")) {
                System.out.println(new String(reply.getData(), 0, reply.getLength()));
            } else {
                System.out.println("Resposta: " + new String(reply.getData(), 0, reply.getLength()));
            }
        } catch (IOException e) {
            System.out.println("Erro de comunicação: " + e.getMessage());
        }
    }
	
}
