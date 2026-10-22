import java.net.*;
import java.util.*;

public class UDPClient {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Uso: java UDPClient <serverHost> <serverPort> <total> [shuffle|manual]");
            return;
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        int total = Integer.parseInt(args[2]);
        boolean manual = args.length > 3 && args[3].equals("manual");
        boolean shuffle = !manual && args.length > 3 && args[3].equals("shuffle");

        DatagramSocket socket = new DatagramSocket();
        socket.setSoTimeout(1000);

        List<Integer> seqs = new ArrayList<>();
        for (int i = 1; i <= total; i++) seqs.add(i);
        if (shuffle) Collections.shuffle(seqs);

        Map<Integer, String> messages = new HashMap<>();
        for (int i = 1; i <= total; i++) messages.put(i, "Message-" + i);

        InetAddress serverAddr = InetAddress.getByName(host);

        try (Scanner scanner = new Scanner(System.in)) {
            for (int i = 0; i < total; i++) {
                int s;
                String payload;
                if (manual) {
                    System.out.print("Número da mensagem: ");
                    s = scanner.nextInt();
                    scanner.nextLine();
                    System.out.print("Mensagem: ");
                    payload = scanner.nextLine();
                } else {
                    s = seqs.get(i);
                    payload = messages.get(s);
                }

                String msg = s + "," + payload;
                byte[] data = msg.getBytes();
                DatagramPacket p = new DatagramPacket(data, data.length, serverAddr, port);
                socket.send(p);
                System.out.println("SENT " + s);

                try {
                    byte[] buf = new byte[1024];
                    DatagramPacket resp = new DatagramPacket(buf, buf.length);
                    socket.receive(resp);
                    String r = new String(resp.getData(), 0, resp.getLength());
                    System.out.println("RECV " + r);
                    if (!manual && r.startsWith("waitingfor,")) {
                        int missing = Integer.parseInt(r.split(",")[1]);
                        String mmsg = missing + "," + messages.get(missing);
                        DatagramPacket pm = new DatagramPacket(mmsg.getBytes(), mmsg.getBytes().length, serverAddr, port);
                        socket.send(pm);
                        System.out.println("RE-SENT missing " + missing);
                    }
                } catch (SocketTimeoutException e) {
                    // sem resposta
                }

                if (!manual) Thread.sleep(150);
            }
        }

        if (!manual) {
            long end = System.currentTimeMillis() + 3000;
            while (System.currentTimeMillis() < end) {
                try {
                    byte[] buf = new byte[1024];
                    DatagramPacket resp = new DatagramPacket(buf, buf.length);
                    socket.receive(resp);
                    String r = new String(resp.getData(), 0, resp.getLength());
                    System.out.println("RECV " + r);
                    if (r.startsWith("waitingfor,")) {
                        int missing = Integer.parseInt(r.split(",")[1]);
                        String mmsg = missing + "," + messages.get(missing);
                        DatagramPacket pm = new DatagramPacket(mmsg.getBytes(), mmsg.getBytes().length, serverAddr, port);
                        socket.send(pm);
                        System.out.println("RE-SENT missing " + missing);
                    }
                } catch (SocketTimeoutException e) {
                    // ignorar
                }
            }
        }

        socket.close();
    }
}
