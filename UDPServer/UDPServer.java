import java.net.*;
import java.io.*;

public class UDPServer {

  public static void main(String args[]) {
    DatagramSocket aSocket = null;

    try {
        aSocket = new DatagramSocket(6789);
        byte[] buffer = new byte[1000];
        int L = 0;
        
        while (true) {
            int N = 0;
            DatagramPacket request = new DatagramPacket(buffer, buffer.length);
            aSocket.receive(request);

            String mensagem = new String(
                request.getData(),
                0,
                request.getLength()
            );

            String[] partes = mensagem.split(",", 2);

            if(partes.length < 2){
                String resposta1 = "Mensagem necessita de ,";
                byte[] resposta1Bytes = resposta1.getBytes();

                DatagramPacket reply = new DatagramPacket(resposta1Bytes, 
                    resposta1Bytes.length, request.getAddress(), request.getPort());

                aSocket.send(reply);
                continue;
            }else{
                try{
                    N = Integer.parseInt(partes[0]);
                }catch(NumberFormatException e){
                    String resposta2 = "Mensagem necessita de numero antes da ,";
                    byte[] resposta2Bytes = resposta2.getBytes();

                    DatagramPacket reply = new DatagramPacket(resposta2Bytes, 
                        resposta2Bytes.length, request.getAddress(), request.getPort());
                    
                    aSocket.send(reply);
                    continue;
                }
                
            }
            
            if(N == L + 1){
                L += 1;
                DatagramPacket reply = new DatagramPacket(request.getData(),
                    request.getLength(), request.getAddress(), request.getPort());

                aSocket.send(reply);
            }else{
                String resposta = "waitingfor," + (L + 1);
                byte[] respostaBytes = resposta.getBytes();

                DatagramPacket reply = new DatagramPacket(respostaBytes,
                    respostaBytes.length, request.getAddress(), request.getPort());

                aSocket.send(reply);
            }

        }
    } catch (SocketException e) { System.out.println("Socket: " + e.getMessage());
    } catch (IOException e)     { System.out.println("IO: " + e.getMessage());
    } finally { if (aSocket != null) aSocket.close(); }
  }
}