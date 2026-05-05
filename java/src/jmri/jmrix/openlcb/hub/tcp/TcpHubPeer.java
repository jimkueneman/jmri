package jmri.jmrix.openlcb.hub.tcp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import jmri.jmrix.openlcb.hub.AbstractHubPeer;
import jmri.jmrix.openlcb.hub.Hub;
import jmri.jmrix.openlcb.hub.HubConfiguration;
import jmri.jmrix.openlcb.hub.HubFrame;
import jmri.jmrix.openlcb.hub.WireFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One connected TCP client. Owns one reader thread (line-oriented from the
 * socket's input stream) and one writer thread (drains the outbound queue
 * and writes ASCII bytes with the configured line ending). All socket writes
 * happen off the hub broadcast path, so a stalled TCP buffer affects only
 * this peer.
 */
public final class TcpHubPeer extends AbstractHubPeer {

    private static final Logger log = LoggerFactory.getLogger(TcpHubPeer.class);

    private final Socket socket;
    private final Thread readerThread;
    private final Thread writerThread;
    private final byte[] frameTerminator;

    public TcpHubPeer(Hub hub, HubConfiguration config, Socket socket) {
        super(hub, config, socket.getRemoteSocketAddress().toString(), WireFormat.GRIDCONNECT);
        this.socket = socket;
        this.frameTerminator = config.sendCrLf
            ? new byte[] { '\r', '\n' }
            : new byte[] { '\n' };

        this.readerThread = new Thread(this::readerLoop,
            "TcpHubPeer-reader-" + remoteAddress());
        this.readerThread.setDaemon(true);

        this.writerThread = new Thread(this::writerLoop,
            "TcpHubPeer-writer-" + remoteAddress());
        this.writerThread.setDaemon(true);
    }

    public void start() {
        readerThread.start();
        writerThread.start();
    }

    @Override
    protected void onFrameQueued() {
        // Writer thread is always blocked in takeOutbound(); ArrayBlockingQueue
        // unblocks it on offer. Nothing to do here.
    }

    @Override
    protected void doClose(String reason) {
        try { socket.close(); } catch (IOException ignored) {}
        readerThread.interrupt();
        writerThread.interrupt();
    }

    private void readerLoop() {
        try (BufferedReader in = new BufferedReader(
                new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII))) {
            String line;
            while (isOpen() && (line = in.readLine()) != null) {
                line = stripGridConnectTerminator(line);
                if (line.isEmpty()) continue;
                if (!acceptInbound(line)) return;  // peer closed (size guard)
            }
        } catch (IOException e) {
            if (isOpen()) log.debug("reader IO error {}: {}", remoteAddress(), e.toString());
        } finally {
            close("reader exit");
        }
    }

    private void writerLoop() {
        try (OutputStream out = socket.getOutputStream()) {
            while (isOpen()) {
                HubFrame frame = takeOutbound();
                long t0 = System.nanoTime();
                writeFrame(out, frame);
                // Drain any additional queued frames into the same TCP segment
                // before flushing — critical under burst load.
                HubFrame more;
                while ((more = pollOutbound()) != null) {
                    writeFrame(out, more);
                }
                out.flush();
                stats().recordWriteLatency(System.nanoTime() - t0);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            if (isOpen()) log.debug("writer IO error {}: {}", remoteAddress(), e.toString());
        } finally {
            close("writer exit");
        }
    }

    private void writeFrame(OutputStream out, HubFrame frame) throws IOException {
        out.write(frame.payload().getBytes(StandardCharsets.US_ASCII));
        out.write(frameTerminator);
    }

    private static String stripGridConnectTerminator(String line) {
        // Tolerate optional CR/LF or stray whitespace; payload is the
        // GridConnect frame ending with ';'.
        int end = line.length();
        while (end > 0) {
            char c = line.charAt(end - 1);
            if (c == '\r' || c == '\n' || c == ' ' || c == '\t') { end--; continue; }
            break;
        }
        return end == line.length() ? line : line.substring(0, end);
    }
}
