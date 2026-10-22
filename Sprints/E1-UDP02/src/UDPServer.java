import java.net.*;
import java.util.*;

public class UDPServer {
    private static final Map<Integer, String> buffer = new HashMap<>();
    private static final List<String> receivedMessages = new ArrayList<>();
    public static void main(String[] args) throws Exception {
        int port = 9876;
        if (args.length > 0) port = Integer.parseInt(args[0]);

        DatagramSocket socket = new DatagramSocket(port);
        System.out.println("Server listening on port " + port);

        int nLastMessageInOrder = 0;

        byte[] buf = new byte[2048];
        while (true) {
            DatagramPacket packet = new DatagramPacket(buf, buf.length);
            socket.receive(packet);

            String msg = new String(packet.getData(), 0, packet.getLength()).trim();
            InetAddress addr = packet.getAddress();
            int portClient = packet.getPort();
            if (!msg.contains(",")) {
                System.out.println("INVALID " + msg + ", L=" + nLastMessageInOrder
                        + ", buffer=" + buffer);
                byte[] invalidResponse = "invalidmessage".getBytes();
                socket.send(new DatagramPacket(invalidResponse, invalidResponse.length, addr, portClient));
                continue;
            }

            String[] parts = msg.split(",", 2);
            int seq;
            try {
                seq = Integer.parseInt(parts[0]);
            } catch (NumberFormatException e) {
                System.out.println("INVALID " + msg + ", L=" + nLastMessageInOrder
                        + ", buffer=" + buffer);
                byte[] invalidResponse = "invalidmessage".getBytes();
                socket.send(new DatagramPacket(invalidResponse, invalidResponse.length, addr, portClient));
                continue;
            }
            String payload = parts.length > 1 ? parts[1] : "";

            int previousLastMessageInOrder = nLastMessageInOrder;
            int deliveredCountBefore = receivedMessages.size();
            nLastMessageInOrder = processDeliveredMessages(
                    nLastMessageInOrder, seq, payload);

            List<String> deliveredThisStep = new ArrayList<>(
                    receivedMessages.subList(deliveredCountBefore, receivedMessages.size()));
            System.out.println("L=" + nLastMessageInOrder
                    + ", buffer=" + buffer
                    + ", delivered=" + deliveredThisStep
                    + ", received=" + receivedMessages);

            String response = nLastMessageInOrder > previousLastMessageInOrder
                    ? msg
                    : "waitingfor," + (nLastMessageInOrder + 1);
            byte[] responseBytes = response.getBytes();
            socket.send(new DatagramPacket(responseBytes, responseBytes.length, addr, portClient));

        }
    }
    public static int processDeliveredMessages(
        int nLastMessageInOrder,

        int nCurrentMessage,
        String currentMessage) {
            if (nCurrentMessage == nLastMessageInOrder + 1) {
                System.out.println("DELIVERED " + nCurrentMessage + " -> " + currentMessage);
                receivedMessages.add(currentMessage);
                nLastMessageInOrder++;
                    while (buffer.containsKey(nLastMessageInOrder + 1)) {
                        String bufferedMessage = buffer.remove(nLastMessageInOrder + 1);
                        receivedMessages.add(bufferedMessage);
                        nLastMessageInOrder++;
}
            } else {
                buffer.put(nCurrentMessage, currentMessage);
                System.out.println("BUFFERED " + nCurrentMessage + " -> " + currentMessage);
            }

            return nLastMessageInOrder;
        }
}
