package jmri.jmrix.openlcb.swing.hub;

import java.awt.BorderLayout;
import java.io.IOException;
import java.net.InetAddress;
import java.text.DateFormat;
import java.util.*;

import javax.swing.*;

import jmri.InstanceManager;
import jmri.UserPreferencesManager;

import jmri.jmrix.can.CanMessage;
import jmri.jmrix.can.CanSystemConnectionMemo;
import jmri.jmrix.can.adapters.gridconnect.GridConnectMessage;
import jmri.jmrix.can.adapters.gridconnect.GridConnectReply;
import jmri.jmrix.can.swing.CanPanelInterface;
import jmri.jmrix.openlcb.hub.AllowAllAuthenticator;
import jmri.jmrix.openlcb.hub.Hub;
import jmri.jmrix.openlcb.hub.HubAuthenticator;
import jmri.jmrix.openlcb.hub.HubConfiguration;
import jmri.jmrix.openlcb.hub.can.CanGatewayHubPeer;
import jmri.jmrix.openlcb.hub.tcp.TcpHubListener;
import jmri.util.swing.JmriJOptionPane;
import jmri.util.zeroconf.ZeroConfServiceManager;

/**
 * Frame displaying — and starting — JMRI's OpenLCB hub.
 *
 * <p>This pane drives the new per-peer hub architecture (see
 * {@code OPENLCB_HUB_CHANGE_REQUEST.md}). On {@code initComponents} it:
 * <ul>
 *   <li>Builds a fresh {@link Hub}.</li>
 *   <li>Registers it (and a default {@link AllowAllAuthenticator}) into
 *       {@link InstanceManager} so the auto-registered
 *       {@link jmri.jmrix.openlcb.hub.websocket.WebSocketHubServlet} at
 *       {@code /lcc} on JMRI's WebServer finds it.</li>
 *   <li>Starts a {@link TcpHubListener} on port 12021 (matching the legacy
 *       hub's default and ZeroConf advertisement).</li>
 *   <li>Bridges the local CAN system to the hub via a single
 *       {@link CanGatewayHubPeer}.</li>
 * </ul>
 *
 * <p>The Swing panel itself is now strictly a UI: it owns no bridging or
 * loopback-prevention logic. Frames flow through {@link Hub#addFrameListener}.
 *
 * @author Bob Jacobsen Copyright (C) 2009, 2010, 2012
 */
public class HubPane extends jmri.util.swing.JmriPanel implements CanPanelInterface {

    public static final int DEFAULT_PORT = 12021;

    public HubPane() {
        this(DEFAULT_PORT);
    }

    public HubPane(int port) {
        this(port, true);
    }

    public HubPane(int port, boolean sendLineEndings) {
        super();
        this.requestedPort = port;
        userPreferencesManager = InstanceManager.getDefault(UserPreferencesManager.class);
        textArea = new javax.swing.JTextArea();
        _send_line_endings = getSendLineEndingsFromUserPref(sendLineEndings);
    }

    private final UserPreferencesManager userPreferencesManager;
    private final int requestedPort;

    private static final String USER_SAVED = ".UserSaved"; // NOI18N
    private static final String USER_SEND_LINE_ENDINGS = ".SendLineTermination"; // NOI18N
    private static final String USER_REQUIRE_LINE_ENDINGS = ".RequireLineTermination"; // NOI18N
    private boolean _send_line_endings;

    private boolean getSendLineEndingsFromUserPref(boolean defaultValue) {
        if (userPreferencesManager.getSimplePreferenceState(getClass().getName() + USER_SAVED)) {
            return userPreferencesManager.getSimplePreferenceState(getClass().getName() + USER_SEND_LINE_ENDINGS);
        }
        return defaultValue;
    }

    private boolean getRequireLineEndingsFromUserPref() {
        return userPreferencesManager.getSimplePreferenceState(getClass().getName() + USER_REQUIRE_LINE_ENDINGS);
    }

    CanSystemConnectionMemo memo;

    transient Hub hub;
    transient HubConfiguration hubConfig;
    transient TcpHubListener tcpListener;
    transient CanGatewayHubPeer canGateway;
    private transient Hub.FrameListener frameLogger;

    /**
     * ZeroConf service type to advertise. Subclasses (e.g. {@code CbusHubPane})
     * override this value before {@link #initComponents} to publish a different
     * service. Default is {@link TcpHubListener#OPENLCB_ZEROCONF_TYPE}.
     */
    protected String zero_conf_addr = TcpHubListener.OPENLCB_ZEROCONF_TYPE;

    @Override
    public void initContext(Object context) {
        log.trace("initContext");
        if (context instanceof CanSystemConnectionMemo) {
            initComponents((CanSystemConnectionMemo) context);
        }
    }

    private final javax.swing.JTextArea textArea;

    @Override
    public void initComponents(CanSystemConnectionMemo memo) {
        log.trace("initComponents");
        this.memo = memo;

        hubConfig = HubConfiguration.builder()
            .tcpPort(requestedPort)
            .sendCrLf(_send_line_endings)
            .requireLineTermination(getRequireLineEndingsFromUserPref())
            .build();

        hub = new Hub();
        HubAuthenticator auth = new AllowAllAuthenticator();

        InstanceManager.setDefault(Hub.class, hub);
        InstanceManager.setDefault(HubConfiguration.class, hubConfig);
        InstanceManager.setDefault(HubAuthenticator.class, auth);

        // GUI
        setLayout(new BorderLayout());
        textArea.setEditable(false);
        add(BorderLayout.CENTER, new JScrollPane(textArea));

        textArea.append(Bundle.getMessage("HubStarted",
            DateFormat.getDateTimeInstance().format(new Date()), getTitle()));
        textArea.append(System.lineSeparator() + Bundle.getMessage("SendLineTermination")
            + " : " + _send_line_endings);
        textArea.append(System.lineSeparator() + Bundle.getMessage("RequireLineTermination")
            + " : " + getRequireLineEndingsFromUserPref());

        // Render every hub frame into the textArea (replaces the old hub's
        // notifyOwner). Runs on the broadcasting thread; we marshal to the
        // EDT and never block.
        frameLogger = (frame, source) -> {
            String line = frame.payload();
            String src = source == null ? "?" : source.remoteAddress();
            SwingUtilities.invokeLater(() ->
                textArea.append(System.lineSeparator()
                    + DateFormat.getDateTimeInstance().format(new Date())
                    + " " + src + " " + line));
        };
        hub.addFrameListener(frameLogger);

        // TCP listener (with ZeroConf — service type set by subclass via zero_conf_addr)
        tcpListener = new TcpHubListener(hub, hubConfig, auth, true, zero_conf_addr);
        try {
            tcpListener.start();
        } catch (IOException e) {
            log.error("Could not start TCP hub on port {}", hubConfig.tcpPort, e);
            textArea.append(System.lineSeparator()
                + "ERROR: TCP hub did not start on port " + hubConfig.tcpPort + ": " + e.getMessage());
        }

        // CAN gateway peer — bridges the local CAN bus into the hub. Subclasses
        // can swap GridConnect message/reply factories via getMessageFrom() and
        // getBlankReply().
        canGateway = new CanGatewayHubPeer(hub, hubConfig,
                memo.getTrafficController(), memo.getUserName()) {
            @Override protected GridConnectMessage newGridConnectMessage(CanMessage m) {
                return getMessageFrom(m);
            }
            @Override protected GridConnectReply newBlankReply() {
                return HubPane.this.getBlankReply();
            }
        };
        canGateway.start();

        addInetAddresses();
    }

    private void addInetAddresses() {
        var t = jmri.util.ThreadingUtil.newThread(() -> {
                log.trace("start addInetAddresses");
                ZeroConfServiceManager manager = InstanceManager.getDefault(ZeroConfServiceManager.class);
                Set<InetAddress> addresses = manager.getAddresses(ZeroConfServiceManager.Protocol.All, true, true);
                for (InetAddress ha : addresses) {
                    var hostAddress = ha.getHostAddress();
                    var hostName = ha.getHostName();
                    var hostNameDup = !hostAddress.equals(hostName) ? hostName : "";
                    var isLoopBack = ha.isLoopbackAddress() ? " Loopback" : ""; // NOI18N
                    var isLinkLocal = ha.isLinkLocalAddress() ? " LinkLocal" : ""; // NOI18N
                    var port = String.valueOf(hubConfig.tcpPort);

                    jmri.util.ThreadingUtil.runOnGUIEventually(() -> {
                        textArea.append(System.lineSeparator() + Bundle.getMessage("IpAddressLine",
                            hostNameDup, isLoopBack, isLinkLocal, hostAddress, port));
                        log.trace("    added a line");
                    });
                }
                log.trace("end addInetAddresses");
            },
            memo.getUserName() + " Hub Thread");
        t.start();
    }

    @Override
    public String getTitle() {
        if (memo != null) {
            return Bundle.getMessage("HubControl", memo.getUserName());
        }
        return "LCC / OpenLCB Hub Control";
    }

    @Override
    public List<JMenu> getMenus() {
        List<JMenu> menuList = new ArrayList<>();
        menuList.add(getLineTerminationSettingsMenu());
        return menuList;
    }

    private JMenu getLineTerminationSettingsMenu() {
        JMenu menu = new JMenu(Bundle.getMessage("LineTermination"));
        JMenuItem sendLineFeedItem = new JMenuItem(Bundle.getMessage("SendLineTermination"));
        sendLineFeedItem.addActionListener(this::showSendTerminationDialog);
        menu.add(sendLineFeedItem);

        JMenuItem requireLineFeedItem = new JMenuItem(Bundle.getMessage("RequireLineTermination"));
        requireLineFeedItem.addActionListener(this::showRequireTerminationDialog);
        menu.add(requireLineFeedItem);

        return menu;
    }

    void showSendTerminationDialog(java.awt.event.ActionEvent e) {
        JCheckBox checkbox = new JCheckBox(Bundle.getMessage("SendLineTermination"));
        checkbox.setSelected(_send_line_endings);
        Object[] params = {Bundle.getMessage("LineTermSettingDialog"), checkbox};
        int result = JmriJOptionPane.showConfirmDialog(this,
            params,
            Bundle.getMessage("SendLineTermination"),
            JmriJOptionPane.OK_CANCEL_OPTION);
        if (result == JmriJOptionPane.OK_OPTION) {
            _send_line_endings = checkbox.isSelected();
            userPreferencesManager.setSimplePreferenceState(getClass().getName() + USER_SAVED, true);
            userPreferencesManager.setSimplePreferenceState(getClass().getName() + USER_SEND_LINE_ENDINGS, _send_line_endings);
        }
    }

    void showRequireTerminationDialog(java.awt.event.ActionEvent e) {
        JCheckBox checkbox = new JCheckBox(Bundle.getMessage("RequireLineTermination"));
        checkbox.setSelected(this.getRequireLineEndingsFromUserPref());
        Object[] params = {Bundle.getMessage("LineTermSettingDialog"), checkbox};
        int result = JmriJOptionPane.showConfirmDialog(this,
            params,
            Bundle.getMessage("RequireLineTermination"),
            JmriJOptionPane.OK_CANCEL_OPTION);
        if (result == JmriJOptionPane.OK_OPTION) {
            userPreferencesManager.setSimplePreferenceState(getClass().getName() + USER_REQUIRE_LINE_ENDINGS, checkbox.isSelected());
        }
    }

    /**
     * Override to provide a wire-format-specific GridConnect encoding for
     * outbound CAN messages. Default produces standard GridConnect.
     */
    protected GridConnectMessage getMessageFrom(CanMessage m) {
        return new GridConnectMessage(m);
    }

    /**
     * Override to provide a wire-format-specific GridConnect decoder for
     * inbound lines from the hub. Default produces standard GridConnect.
     */
    protected GridConnectReply getBlankReply() {
        return new GridConnectReply();
    }

    @Override
    public void dispose() {
        if (canGateway != null) {
            canGateway.close("HubPane dispose");
            canGateway = null;
        }
        if (tcpListener != null) {
            tcpListener.stop();
            tcpListener = null;
        }
        if (hub != null) {
            if (frameLogger != null) hub.removeFrameListener(frameLogger);
            hub.closeAll("HubPane dispose");
            // Clear InstanceManager defaults so the WebSocket servlet stops
            // serving frames once HubPane is closed.
            if (InstanceManager.getNullableDefault(Hub.class) == hub) {
                InstanceManager.deregister(hub, Hub.class);
            }
            hub = null;
        }
    }

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(HubPane.class);
}
