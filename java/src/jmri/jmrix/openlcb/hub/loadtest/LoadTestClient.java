package jmri.jmrix.openlcb.hub.loadtest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Minimal TCP load-generator. Connects to the hub, then runs one of:
 *   normal  - 1 frame/sec, prints any received frames
 *   burst   - 10000 frames as fast as possible, then idle
 *   slow    - reads only 1 frame/sec, lets outbound queue fill
 *   flood   - inbound flood; the new hub paces this back via TCP, never closes
 *
 * Usage: java -cp ... LoadTestClient host port mode [tag]
 */
public final class LoadTestClient {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("usage: LoadTestClient <host> <port> <normal|burst|slow|flood> [tag]");
            System.exit(2);
        }
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String mode = args[2];
        String tag = args.length > 3 ? args[3] : "C";

        try (Socket socket = new Socket(host, port)) {
            socket.setTcpNoDelay(true);
            BufferedReader in = new BufferedReader(
                new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), false, StandardCharsets.US_ASCII);

            switch (mode) {
                case "normal": runNormal(in, out, tag); break;
                case "burst":  runBurst(in, out, tag);  break;
                case "slow":   runSlow(in, out, tag);   break;
                case "flood":  runFlood(in, out, tag);  break;
                default:
                    System.err.println("unknown mode: " + mode);
                    System.exit(2);
            }
        }
    }

    private static void runNormal(BufferedReader in, PrintWriter out, String tag) throws Exception {
        AtomicLong rxCount = new AtomicLong();
        Thread reader = startReader(in, rxCount, tag, false);
        long n = 0;
        while (!Thread.currentThread().isInterrupted()) {
            String frame = ":X19490AAAN" + tag + String.format("%015X", n++ & 0xFFFFFFFFFFFFFFFL) + ";";
            out.println(frame);
            out.flush();
            Thread.sleep(1000);
        }
        reader.interrupt();
    }

    private static void runBurst(BufferedReader in, PrintWriter out, String tag) throws Exception {
        AtomicLong rxCount = new AtomicLong();
        Thread reader = startReader(in, rxCount, tag, false);
        System.out.println("[" + tag + "] sending 10000 frames as fast as possible...");
        long t0 = System.nanoTime();
        for (long i = 0; i < 10000; i++) {
            String frame = ":X19490AAAN" + tag + String.format("%015X", i) + ";";
            out.println(frame);
        }
        out.flush();
        long ms = (System.nanoTime() - t0) / 1_000_000;
        System.out.println("[" + tag + "] burst sent in " + ms + " ms");

        Thread.sleep(2000);
        System.out.println("[" + tag + "] received " + rxCount.get() + " frames");
        reader.interrupt();
    }

    private static void runSlow(BufferedReader in, PrintWriter out, String tag) throws Exception {
        AtomicLong rxCount = new AtomicLong();
        Thread reader = startReader(in, rxCount, tag, true);
        long n = 0;
        while (!Thread.currentThread().isInterrupted()) {
            String frame = ":X19490AAAN" + tag + String.format("%015X", n++) + ";";
            out.println(frame);
            out.flush();
            Thread.sleep(1000);
        }
        reader.interrupt();
    }

    private static void runFlood(BufferedReader in, PrintWriter out, String tag) throws Exception {
        AtomicLong rxCount = new AtomicLong();
        Thread reader = startReader(in, rxCount, tag, false);
        System.out.println("[" + tag + "] flooding (hub will pace via TCP backpressure)");
        long sent = 0;
        long t0 = System.nanoTime();
        try {
            while (!Thread.currentThread().isInterrupted()) {
                String frame = ":X19490AAAN" + tag + String.format("%015X", sent) + ";";
                out.println(frame);
                if ((sent & 0xFFF) == 0) out.flush();
                if (out.checkError()) break;
                sent++;
            }
        } finally {
            long ms = Math.max(1, (System.nanoTime() - t0) / 1_000_000);
            System.out.println("[" + tag + "] sent " + sent + " ("
                + (sent * 1000 / ms) + " fps)");
        }
        reader.interrupt();
    }

    private static Thread startReader(BufferedReader in, AtomicLong count, String tag, boolean throttled) {
        Thread t = new Thread(() -> {
            try {
                String line;
                while ((line = in.readLine()) != null) {
                    long c = count.incrementAndGet();
                    if (c <= 5 || c % 1000 == 0) {
                        System.out.println("[" + tag + "] rx#" + c + " " + line);
                    }
                    if (throttled) {
                        try { Thread.sleep(1000); } catch (InterruptedException e) { return; }
                    }
                }
            } catch (IOException e) {
                System.out.println("[" + tag + "] reader ended: " + e.getMessage());
            }
        }, "loadtest-reader-" + tag);
        t.setDaemon(true);
        t.start();
        return t;
    }
}
