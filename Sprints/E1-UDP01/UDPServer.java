import java.net.*;


public class UDPServer {
    public static void main(String[] args) {
        DatagramSocket aSocket = null;
        try {
            aSocket = new DatagramSocket(6789);
            byte[] buffer = new byte[1000];
            int L = 0;
            while (true) {
                DatagramPacket request = new DatagramPacket(buffer, buffer.length);
                aSocket.receive(request);
                String message = new String(request.getData(), 0, request.getLength());
                if (!message.split(",")[0].isEmpty() && message.split(",")[0].matches("\\d+") && message.split(",").length > 1) {
                    if (message.split(",")[0].equals(String.valueOf(L+1))) {
                        System.out.println("Recebido: " + new String(request.getData(), 0, request.getLength()));
                        L++;
                        DatagramPacket reply = new DatagramPacket(request.getData(), request.getLength(), request.getAddress(), request.getPort());
                        aSocket.send(reply);
                    } else {
                        String replyMessage = "waitingfor," + (L+1);
                        DatagramPacket reply = new DatagramPacket(replyMessage.getBytes(), replyMessage.length(), request.getAddress(), request.getPort());
                        aSocket.send(reply);
                    }
                    }
                else {
                    System.out.println("Mensagem inválida recebida: " + message);
                    String replyMessage = "invalidmessage";
                    DatagramPacket reply = new DatagramPacket(replyMessage.getBytes(), replyMessage.length(), request.getAddress(), request.getPort());
                    aSocket.send(reply);
                }
                
            }
        }
        catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
        finally {
            if (aSocket != null) aSocket.close();
        }
    }

}