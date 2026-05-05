package jmri.jmrix.openlcb.hub.itest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

/**
 * In-process TCP client used by integration scenarios. Tracks received-frame
 * count and exposes a simple send method. Optional read throttle simulates a
 * slow consumer.
 */
public final class TcpTestPeer implements AutoCloseable {

    private final Socket socket;
    private final BufferedReader in;
    private final PrintWriter out;
    private final AtomicLong rxCount = new AtomicLong();
    private final Thread readerThread;
    private volatile int readDelayMs;
    private volatile boolean closed;

    public TcpTestPeer(String host, int port, String tag) throws IOException {
        socket = new Socket(host, port);
        socket.setTcpNoDelay(true);
        in = new BufferedReader(
            new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII));
        out = new PrintWriter(socket.getOutputStream(), false, StandardCharsets.US_ASCII);
        readerThread = new Thread(this::readLoop, "TcpTestPeer-" + tag);
        readerThread.setDaemon(true);
        readerThread.start();
    }

    private void readLoop() {
        try {
            String line;
            while ((line = in.readLine()) != null) {
                rxCount.incrementAndGet();
                int delay = readDelayMs;
                if (delay > 0) {
                    try { Thread.sleep(delay); } catch (InterruptedException e) { return; }
                }
            }
        } catch (IOException ignored) {
            // socket closed
        }
    }

    public void send(String frame) {
        out.println(frame);
        out.flush();
    }

    /** Slow this peer's reader to simulate a slow consumer. 0 = no delay. */
    public void setReadDelayMs(int ms) { this.readDelayMs = ms; }

    public long received() { return rxCount.get(); }
    public boolean isOpen() { return !closed && !socket.isClosed(); }
    public boolean writeError() { return out.checkError(); }

    @Override
    public void close() {
        closed = true;
        try { socket.close(); } catch (IOException ignored) {}
        readerThread.interrupt();
    }
}
