import java.net.*;
import java.io.*;
import java.util.Scanner;

public class UDPClient {

  public static void main(String args[]) {
    DatagramSocket aSocket = null;
    Scanner scanner = null;

    try {
        aSocket = new DatagramSocket();
        scanner = new Scanner(System.in);
        String mensagem;
        int numero;
        int N = 1;

        while(true){
            System.out.print("Modo (automatico/manual): ");
            String modo = scanner.nextLine();

            if(modo.equalsIgnoreCase("automatico")){
                while(modo.equalsIgnoreCase("automatico")){
                    System.out.print("Mensagem: " + N + ",");
                    mensagem = scanner.nextLine();

                    if(mensagem.equalsIgnoreCase("sair")){
                        break;
                    }

                    mensagem = N + "," + mensagem;

                    byte[] m = mensagem.getBytes();
                    InetAddress aHost = InetAddress.getByName("localhost");
                    int serverPort = 6789;

                    DatagramPacket request = new DatagramPacket(m, m.length, aHost, serverPort);
                    aSocket.send(request);

                    byte[] buffer = new byte[1000];

                    DatagramPacket reply = new DatagramPacket(buffer, buffer.length);
                    aSocket.receive(reply);

                    String replyString = new String(
                        reply.getData(),
                        0,
                        reply.getLength()
                    );
                    
                    if(replyString.startsWith("waitingfor,")){
                        System.out.println("Reply: " + replyString);
                    }else{
                        System.out.println("Echo: " + replyString);
                    }
                    
                    N += 1;
                }
            }else if (modo.equalsIgnoreCase("manual")){
                while(modo.equalsIgnoreCase("manual")){
                    System.out.print("Numero: ");

                    try{
                       numero = Integer.parseInt(scanner.nextLine()); 
                    }catch(NumberFormatException e){
                        System.out.println("Numero invalido");
                        continue;
                    }

                    System.out.print("Mensagem: ");
                    mensagem = scanner.nextLine();

                    if(mensagem.equalsIgnoreCase("sair")){
                        break;
                    }

                    mensagem = numero + "," + mensagem;

                    byte[] m = mensagem.getBytes();
                    InetAddress aHost = InetAddress.getByName("localhost");
                    int serverPort = 6789;

                    DatagramPacket request = new DatagramPacket(m, m.length, aHost, serverPort);
                    aSocket.send(request);

                    byte[] buffer = new byte[1000];

                    DatagramPacket reply = new DatagramPacket(buffer, buffer.length);
                    aSocket.receive(reply);
                    
                    String replyString = new String(
                        reply.getData(),
                        0,
                        reply.getLength()
                    );
                    
                    if(replyString.startsWith("waitingfor,")){
                        System.out.println("Reply: " + replyString);
                    }else{
                        System.out.println("Echo: " + replyString);
                    }

                    
                }
            }else if (modo.equalsIgnoreCase("sair")){
                break;
            }else{
                System.out.println("Modo inválido.");
            }
        }
    } catch (SocketException e) { System.out.println("Socket: " + e.getMessage());
    } catch (IOException e)     { System.out.println("IO: " + e.getMessage());
    } finally { 
        if (aSocket != null) aSocket.close(); 
        if (scanner != null) scanner.close();
    }
  }
}