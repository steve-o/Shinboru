/* Simple provider.
 */

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.*;
import java.net.*;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
// Java 8
import java.time.*;
import java.time.format.*;
import java.time.temporal.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.thomsonreuters.upa.codec.Array;
import com.thomsonreuters.upa.codec.ArrayEntry;
import com.thomsonreuters.upa.codec.Buffer;
import com.thomsonreuters.upa.codec.Codec;
import com.thomsonreuters.upa.codec.CodecFactory;
import com.thomsonreuters.upa.codec.CodecReturnCodes;
import com.thomsonreuters.upa.codec.DataDictionary;
import com.thomsonreuters.upa.codec.DataStates;
import com.thomsonreuters.upa.codec.DataTypes;
import com.thomsonreuters.upa.codec.DictionaryEntry;
import com.thomsonreuters.upa.codec.DecodeIterator;
import com.thomsonreuters.upa.codec.ElementEntry;
import com.thomsonreuters.upa.codec.ElementList;
import com.thomsonreuters.upa.codec.ElementListFlags;
import com.thomsonreuters.upa.codec.EncodeIterator;
import com.thomsonreuters.upa.codec.FieldEntry;
import com.thomsonreuters.upa.codec.FieldList;
import com.thomsonreuters.upa.codec.FieldListFlags;
import com.thomsonreuters.upa.codec.FilterEntry;
import com.thomsonreuters.upa.codec.FilterEntryActions;
import com.thomsonreuters.upa.codec.FilterEntryFlags;
import com.thomsonreuters.upa.codec.FilterList;
import com.thomsonreuters.upa.codec.GenericMsg;
import com.thomsonreuters.upa.codec.GenericMsgFlags;
import com.thomsonreuters.upa.codec.LocalFieldSetDefDb;
import com.thomsonreuters.upa.codec.MapEntry;
import com.thomsonreuters.upa.codec.MapEntryActions;
import com.thomsonreuters.upa.codec.Msg;
import com.thomsonreuters.upa.codec.MsgClasses;
import com.thomsonreuters.upa.codec.MsgKey;
import com.thomsonreuters.upa.codec.MsgKeyFlags;
import com.thomsonreuters.upa.codec.RefreshMsg;
import com.thomsonreuters.upa.codec.RefreshMsgFlags;
import com.thomsonreuters.upa.codec.RequestMsg;
import com.thomsonreuters.upa.codec.RequestMsgFlags;
import com.thomsonreuters.upa.codec.Series;
import com.thomsonreuters.upa.codec.SeriesEntry;
import com.thomsonreuters.upa.codec.SeriesFlags;
import com.thomsonreuters.upa.codec.State;
import com.thomsonreuters.upa.codec.StateCodes;
import com.thomsonreuters.upa.codec.StatusMsg;
import com.thomsonreuters.upa.codec.StatusMsgFlags;
import com.thomsonreuters.upa.codec.StreamStates;
import com.thomsonreuters.upa.codec.UpdateMsg;
import com.thomsonreuters.upa.codec.UpdateMsgFlags;
import com.thomsonreuters.upa.rdm.Dictionary;
import com.thomsonreuters.upa.rdm.Directory;
import com.thomsonreuters.upa.rdm.DomainTypes;
import com.thomsonreuters.upa.rdm.ElementNames;
import com.thomsonreuters.upa.rdm.InstrumentNameTypes;
import com.thomsonreuters.upa.rdm.Login;
import com.thomsonreuters.upa.transport.Channel;
import com.thomsonreuters.upa.transport.ChannelInfo;
import com.thomsonreuters.upa.transport.ChannelState;
import com.thomsonreuters.upa.transport.ComponentInfo;
import com.thomsonreuters.upa.transport.CompressionTypes;
import com.thomsonreuters.upa.transport.ConnectOptions;
import com.thomsonreuters.upa.transport.ConnectionTypes;
import com.thomsonreuters.upa.transport.InProgFlags;
import com.thomsonreuters.upa.transport.InProgInfo;
import com.thomsonreuters.upa.transport.ReadArgs;
import com.thomsonreuters.upa.transport.Transport;
import com.thomsonreuters.upa.transport.TransportBuffer;
import com.thomsonreuters.upa.transport.TransportFactory;
import com.thomsonreuters.upa.transport.TransportReturnCodes;
import com.thomsonreuters.upa.transport.WriteArgs;
import com.thomsonreuters.upa.transport.WriteFlags;
import com.thomsonreuters.upa.transport.WritePriorities;

public class Provider {
	private static Logger LOG = LogManager.getLogger (Provider.class.getName());
	private static final String LINE_SEPARATOR = System.getProperty ("line.separator");

	private SessionConfig config;

/* UPA context. */
	private Upa upa;
/* This flag is set to false when Run should return. */
	private boolean keep_running;

/* Active UPA connection. */
	private Channel connection;
/* unique id per connection. */
	private String prefix;

	private StringBuilder sb;

/* Pending messages to flush. */
	int pending_count;

/* Callback interface for publishing universe on service "operationally up". */
	public interface Delegate {
		public boolean OnRequest (Channel c, int service_id, ItemStream stream, EncodeIterator it, TransportBuffer buf);
	}

	private Delegate delegate;

/* Watchlist of all items. */
	private Map<String, ItemStream> directory;

/* The integer number the service name is mapped to in the source directory. */
	private int service_id;

/* incrementing unique id for streams */
	int token;
	int login_token;	/* should always be -1 */
	int directory_token;
	private BiMap<String, Integer> dictionary_tokens;

/* RSSL keepalive state. */
	Instant next_ping;
	Instant next_pong;
	int ping_interval;	/* seconds */

	private Instant last_activity;

/* Data dictionaries. */
	private DataDictionary rdm_dictionary;

/* Reuters Wire Format versions. */
	private byte rwf_major_version;
	private byte rwf_minor_version;

	private boolean is_muted;
	private boolean pending_dictionary;

	private static final boolean CLOSE_ON_SHUTDOWN = false;
	private static final boolean UNSUBSCRIBE_ON_SHUTDOWN = false;

	private static final int DEFAULT_SERVICE_ID		= 1;

	private static final int MAX_MSG_SIZE			= 4096;
	private static final int DEFAULT_RSSL_PORT		= 14003;	/* non-interactive */
	private static final int DEFAULT_STREAM_IDENTIFIER	= -1;		/* non-interactive */
	private static final int INVALID_STREAM_IDENTIFIER	= 0;

	private static final String RSSL_PROTOCOL		= "rssl";

/* Hard coded for Elektron and TREP. */
	private static final String FIELD_DICTIONARY_NAME 	= "RWFFld";
	private static final String ENUM_TYPE_DICTIONARY_NAME 	= "RWFEnum";

	private Instant NextPing() {
		return this.next_ping;
	}

	private Instant NextPong() {
		return this.next_pong;
	}

	private void SetNextPing (Instant time) {
		this.next_ping = time;
	}

	private void SetNextPong (Instant time) {
		this.next_pong = time;
	}

	private void IncrementPendingCount() {
		this.pending_count++;
	}

	private void ClearPendingCount() {
		this.pending_count = 0;
	}

	private int GetPendingCount() {
		return this.pending_count;
	}

	public Provider (SessionConfig config, Upa upa, Delegate delegate) {
		this.config = config;
		this.upa = upa;
		this.delegate = delegate;
		this.service_id = DEFAULT_SERVICE_ID;
		this.rwf_major_version = 0;
		this.rwf_minor_version = 0;
		this.is_muted = true;
		this.keep_running = true;
		this.pending_dictionary = true;
	}

	public boolean Initialize() {
		LOG.trace (this.config);

/* Manual serialisation */
		this.sb = new StringBuilder (512);

/* RSSL Version Info. */
		if (!this.upa.VerifyVersion()) {
			return false;
		}

		this.directory = new LinkedHashMap<String, ItemStream>();
		this.dictionary_tokens = HashBiMap.create();
		this.rdm_dictionary = CodecFactory.createDataDictionary();
		return true;
	}

/* Tokens are valid for the duration of an active session, for non-interactive
 * providers they should be negative, for interactive providers and consumers
 * they should be positive.
 */
	private int GenerateToken() {
		return this.token--;
	}

	public void Close() {
	}

	private Selector selector;
	private Set<SelectionKey> out_keys;

	public void Run() {
		assert this.keep_running : "Quit must have been called outside of Run!";
		LOG.trace ("Run");

// throws IOException for undocumented reasons.
		try {
			this.selector = Selector.open();
		} catch (IOException e) {
			LOG.catching (e);
			this.keep_running = true;
			return;
		}
		this.out_keys = null;
		final long timeout = 100 * 1000;	/* milliseconds */

		while (true) {
			boolean did_work = this.DoWork();
			if (!this.keep_running)
				break;

			if (did_work)
				continue;

			try {
				final int rc = this.selector.select (timeout /* milliseconds */);
				if (rc > 0) {
					this.out_keys = this.selector.selectedKeys();
				} else {
					this.out_keys = null;
				}
			} catch (Exception e) {
				LOG.catching (e);
			}
		}

		this.keep_running = true;
		LOG.trace ("Mainloop deactivated.");
	}

	public void Quit() {
		this.keep_running = false;
	}

	private boolean DoWork() {
		LOG.trace ("DoWork");
		boolean did_work = false;

		this.last_activity = Instant.now();

/* Only check keepalives on timeout */
		if (null == this.out_keys
			&& null != this.connection	/* before first connection attempt */
			&& ChannelState.INACTIVE != this.connection.state())	/* not shutdown */
		{
			final Channel c = this.connection;
			LOG.debug ("timeout, state {}", ChannelState.toString (c.state()));
			if (ChannelState.ACTIVE == c.state()) {
				if (this.last_activity.isAfter (this.NextPing())) {
					this.Ping (c);
				}
				if (this.last_activity.isAfter (this.NextPong())) {
					LOG.error ("Pong timeout from peer, aborting connection.");
					this.Abort (c);
				}
			}
			return false;
		}

/* Client connection */
		if (null == this.connection
			|| ChannelState.INACTIVE == this.connection.state())
		{
			this.Connect();
/* In UPA/Java we return false in order to avoid timeout state on protocol downgrade. */
			did_work = false;
		}

		if (null != this.connection
			&& null != this.out_keys)
		{
			final Channel c = this.connection;
			Iterator<SelectionKey> it = this.selector.selectedKeys().iterator();
			while (it.hasNext()) {
				final SelectionKey key = it.next();
				key.attach (Boolean.TRUE);
/* connected */
				if (key.isValid() && key.isConnectable()) {
					key.attach (Boolean.FALSE);
					this.OnCanConnectWithoutBlocking (c);
					did_work = true;
				}
/* incoming */
				if (key.isValid() && key.isReadable()) {
					key.attach (Boolean.FALSE);
					this.OnCanReadWithoutBlocking (c);
					did_work = true;
				}
/* outgoing */
				if (key.isValid() && key.isWritable()) {
					key.attach (Boolean.FALSE);
					this.OnCanWriteWithoutBlocking (c);
					did_work = true;
				}
				if (Boolean.FALSE.equals (key.attachment())) {
					it.remove();
				}
			}
/* Keepalive timeout on active session above connection */
			if (ChannelState.ACTIVE == c.state()) {
				if (this.last_activity.isAfter (this.NextPing())) {
					this.Ping (c);
				}
				if (this.last_activity.isAfter (this.NextPong())) {
					LOG.error ("Pong timeout from peer, aborting connection.");
					this.Abort (c);
				}
			}
/* disconnects */
		}

		return did_work;
	}

/* Basic round-robin cold-standby failover support. */
	private int server_idx = -1;

	private String server() {
		this.server_idx++;
		if (this.server_idx >= this.config.getServers().length)
			this.server_idx = 0;
		return this.config.getServers()[this.server_idx];
	}

	private void Connect() {
		final ConnectOptions addr = TransportFactory.createConnectOptions();
		final com.thomsonreuters.upa.transport.Error rssl_err = TransportFactory.createError();

		LOG.info ("Initiating new connection.");

/* non-blocking mode to be used with a Selector. */
		addr.blocking (false);
		addr.channelReadLocking (false);
		addr.channelWriteLocking (false);
		addr.unifiedNetworkInfo().address (this.server());
		addr.unifiedNetworkInfo().serviceName (this.config.hasDefaultPort() ? this.config.getDefaultPort() : Integer.toString (DEFAULT_RSSL_PORT));
		addr.protocolType (Codec.protocolType());
		addr.majorVersion (Codec.majorVersion());
		addr.minorVersion (Codec.minorVersion());
		final Channel c = Transport.connect (addr, rssl_err);
		if (null == c) {
			LOG.error ("Transport.connect: { \"errorId\": {}, \"sysError\": {}, \"text\": \"{}\", \"connectionInfo\": {}, \"protocolType\": {}, \"majorversion\": {}, \"minorVersion\": {} }",
				rssl_err.errorId(), rssl_err.sysError(), rssl_err.text(),
				addr.unifiedNetworkInfo(), addr.protocolType(), addr.majorVersion(), addr.minorVersion());
		} else {
			this.connection = c;
/* Set logger ID */
			this.prefix = Integer.toHexString (c.hashCode());

/* Wait for session */
			try {
				c.selectableChannel().register (this.selector, SelectionKey.OP_CONNECT | SelectionKey.OP_READ | SelectionKey.OP_WRITE, Boolean.FALSE);
			} catch (ClosedChannelException e) {
/* leave error handling to Channel wrapper */
				LOG.catching (e);
			}

			LOG.info ("RSSL socket created: { \"connectionInfo\": {}, \"connectionType\": \"{}\", \"majorVersion\": {}, \"minorVersion\": {}, \"pingTimeout\": {}, \"protocolType\": {}, \"socketId\": {}, \"state\": \"{}\" }",
				addr.unifiedNetworkInfo(),
				ConnectionTypes.toString (c.connectionType()), c.majorVersion(), c.minorVersion(), c.pingTimeout(), c.protocolType(), c.selectableChannel().hashCode(), ChannelState.toString (c.state()));
		}
	}

	private void OnCanConnectWithoutBlocking (Channel c) {
		LOG.trace ("OnCanConnectWithoutBlocking");
		switch (c.state()) {
		case ChannelState.CLOSED:
			LOG.info ("socket state is closed.");
			this.Abort (c);
			break;
		case ChannelState.INACTIVE:
			LOG.info ("socket state is inactive.");
			break;
		case ChannelState.INITIALIZING:
			LOG.info ("socket state is initializing.");
			this.OnInitializingState (c);
			break;
		default:
			LOG.info ("unhandled socket state.");
			break;
		}
	}

	private void OnCanReadWithoutBlocking (Channel c) {
		LOG.trace ("OnCanReadWithoutBlocking");
		switch (c.state()) {
		case ChannelState.CLOSED:
			LOG.info ("socket state is closed.");
/* Raise internal exception flags to remove socket */
			this.Abort (c);
			break;
		case ChannelState.INACTIVE:
			LOG.info ("socket state is inactive.");
			break;
		case ChannelState.INITIALIZING:
			LOG.info ("socket state is initializing.");
			break;
		case ChannelState.ACTIVE:
			this.OnActiveReadState (c);
			break;
		default:
			LOG.error ("socket state is unknown.");
			break;
		}
	}

	private void OnInitializingState (Channel c) {
		final InProgInfo state = TransportFactory.createInProgInfo();
		final com.thomsonreuters.upa.transport.Error rssl_err = TransportFactory.createError();

		final int rc = c.init (state, rssl_err);
		switch (rc) {
		case TransportReturnCodes.CHAN_INIT_IN_PROGRESS:
			if (InProgFlags.SCKT_CHNL_CHANGE == (state.flags() & InProgFlags.SCKT_CHNL_CHANGE)) {
				LOG.info ("RSSL protocol downgrade, reconnected.");
				state.oldSelectableChannel().keyFor (this.selector).cancel();
				try {
					c.selectableChannel().register (this.selector, SelectionKey.OP_CONNECT | SelectionKey.OP_READ | SelectionKey.OP_WRITE, Boolean.FALSE);
				} catch (ClosedChannelException e) {
					LOG.catching (e);
				}
			} else {
				LOG.info ("RSSL connection in progress.");
			}
			break;
		case TransportReturnCodes.SUCCESS:
			this.OnActiveSession (c);
			try {
				c.selectableChannel().register (this.selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE, Boolean.FALSE);
			} catch (ClosedChannelException e) {
				LOG.catching (e);
			}
			break;
		default:
			LOG.error ("Channel.init: { \"errorId\": {}, \"sysError\": \"{}\", \"text\": \"{}\" }",
				rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
			this.Abort (c);
			break;
		}
	}

	private void OnCanWriteWithoutBlocking (Channel c) {
		LOG.trace ("OnCanWriteWithoutBlocking");
		switch (c.state()) {
		case ChannelState.CLOSED:
			LOG.info ("socket state is closed.");
/* Raise internal exception flags to remove socket */
			this.Abort (c);
			break;
		case ChannelState.INACTIVE:
			LOG.info ("socket state is inactive.");
			break;
		case ChannelState.INITIALIZING:
			LOG.info ("socket state is initializing.");
			this.OnInitializingState (c);
			break;
		case ChannelState.ACTIVE:
			this.OnActiveWriteState (c);
			break;
		default:
			LOG.error ("socket state is unknown.");
			break;
		}
	}

	private void OnActiveWriteState (Channel c) {
		final com.thomsonreuters.upa.transport.Error rssl_err = TransportFactory.createError();

		LOG.trace ("rsslFlush");
		final int rc = c.flush (rssl_err);
		if (TransportReturnCodes.SUCCESS == rc) {
			final SelectionKey key = c.selectableChannel().keyFor (selector);
			key.interestOps (key.interestOps() & ~SelectionKey.OP_WRITE);
			this.ClearPendingCount();
			this.SetNextPing (this.last_activity.plusSeconds (this.ping_interval));
		} else if (rc > 0) {
			LOG.info ("{} bytes pending.", rc);
		} else {
			LOG.error ("Channel.flush: { \"errorId\": {}, \"sysError\": \"{}\", \"text\": \"{}\" }",
				rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
		}
	}

	private void Abort (Channel c) {
		this.Close (c);
	}

	private void Close (Channel c) {
		final com.thomsonreuters.upa.transport.Error rssl_err = TransportFactory.createError();

		LOG.info ("Closing RSSL connection.");
		final int rc = c.close (rssl_err);
		if (TransportReturnCodes.SUCCESS != rc) {
			LOG.warn ("Channel.close: { \"errorId\": {}, \"sysError\": \"{}\", \"text\": \"{}\" }",
				rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
		}
	}

	private boolean OnActiveSession (Channel c) {
		final ChannelInfo info = TransportFactory.createChannelInfo();
		final com.thomsonreuters.upa.transport.Error rssl_err = TransportFactory.createError();

		this.last_activity = Instant.now();

/* Relog negotiated state. */
		LOG.info ("RSSL negotiated state: { \"connectionType\": \"{}\", \"majorVersion\": {}, \"minorVersion\": {}, \"pingTimeout\": {}, \"protocolType\": \"{}\", \"socketId\": {}, \"state\": \"{}\" }",
			ConnectionTypes.toString (c.connectionType()), c.majorVersion(), c.minorVersion(), c.pingTimeout(), c.protocolType(), c.selectableChannel(), ChannelState.toString (c.state()));

/* Store negotiated Reuters Wire Format version information. */
		final int rc = c.info (info, rssl_err);
		if (TransportReturnCodes.SUCCESS != rc) {
			LOG.error ("Channel.info: { \"errorId\": {}, \"sysError\": \"{}\", \"text\": \"{}\" }",
				rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
			return false;
		}

/* Log connected infrastructure. */
		final StringBuilder components = new StringBuilder ("[ ");
		final Iterator<ComponentInfo> it = info.componentInfo().iterator();
		while (it.hasNext()) {
			final ComponentInfo component = it.next();
			components.append ("{ ")
				.append ("\"componentVersion\": \"").append (component.componentVersion()).append ("\"")
				.append (" }");
			if (it.hasNext())
				components.append (", ");
		}
		components.append (" ]");

		LOG.info ("channelInfo: { \"clientToServerPings\": {}, \"componentInfo\": {}, \"compressionThreshold\": {}, \"compressionType\": \"{}\", \"guaranteedOutputBuffers\": {}, \"maxFragmentSize\": {}, \"maxOutputBuffers\": {}, \"numInputBuffers\": {}, \"pingTimeout\": {}, \"priorityFlushStrategy\": \"{}\", \"serverToClientPings\": \"{}\", \"sysRecvBufSize\": {}, \"sysSendBufSize\": {} }",
			info.clientToServerPings(), components.toString(), info.compressionThreshold(), info.compressionType(), CompressionTypes.toString (info.compressionType()), info.guaranteedOutputBuffers(), info.maxFragmentSize(), info.maxOutputBuffers(), info.numInputBuffers(), info.pingTimeout(), info.priorityFlushStrategy(), info.serverToClientPings(), info.sysRecvBufSize(), info.sysSendBufSize());
/* First token aka stream id */
		this.token = DEFAULT_STREAM_IDENTIFIER;
		this.dictionary_tokens.clear();
/* Derive expected RSSL ping interval from negotiated timeout. */
		this.ping_interval = c.pingTimeout() / 3;
/* Schedule first RSSL ping. */
		this.SetNextPing (this.last_activity.plusSeconds (this.ping_interval));
/* Treat connect as first RSSL pong. */
		this.SetNextPong (this.last_activity.plusSeconds (c.pingTimeout()));
/* Reset RDM data dictionary and wait to request from upstream. */
		return this.SendLoginRequest (c);
	}

	private void OnActiveReadState (Channel c) {
		final ReadArgs read_args = TransportFactory.createReadArgs();
		final com.thomsonreuters.upa.transport.Error rssl_err = TransportFactory.createError();

		final TransportBuffer buf = c.read (read_args, rssl_err);
		final int rc = read_args.readRetVal();
		if (rc > 0) {
			LOG.debug ("Channel.read: { \"pendingBytes\": {}, \"bytesRead\": {}, \"uncompressedBytesRead\": {}, \"rsslErrorId\": {}, \"sysError\": {}, \"text\": \"{}\" }",
				rc,
				read_args.bytesRead(), read_args.uncompressedBytesRead(), rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
		} else {
			LOG.debug ("Channel.read: { \"returnCode\": {}, \"enumeration\": \"{}\", \"bytesRead\": {}, \"uncompressedBytesRead\": {}, \"rsslErrorId\": {}, \"sysError\": {}, \"text\": \"{}\" }",
				rc, TransportReturnCodes.toString (rc),
				read_args.bytesRead(), read_args.uncompressedBytesRead(), rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
		}

		if (TransportReturnCodes.CONGESTION_DETECTED == rc
			|| TransportReturnCodes.SLOW_READER == rc
			|| TransportReturnCodes.PACKET_GAP_DETECTED == rc)
		{
			if (ChannelState.CLOSED != c.state()) {
				LOG.warn ("Channel.read: { \"errorId\": {}, \"sysError\": \"{}\", \"text\": \"{}\", \"size\": {}, \"packedBuffer\": false }",
					rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
			}
		}
		else if (TransportReturnCodes.READ_FD_CHANGE == rc)
		{
			LOG.info ("RSSL reconnected.");
			c.oldSelectableChannel().keyFor (this.selector).cancel();
			try {
				c.selectableChannel().register (this.selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE, Boolean.FALSE);
			} catch (ClosedChannelException e) {
				LOG.catching (e);
			}
		}
		else if (TransportReturnCodes.READ_PING == rc)
		{
			this.SetNextPong (this.last_activity.plusSeconds (c.pingTimeout()));
			LOG.info ("RSSL pong.");
		}
		else if (TransportReturnCodes.FAILURE == rc)
		{
			LOG.error ("Channel.read: { \"errorId\": {}, \"sysError\": \"{}\", \"text\": \"{}\", \"size\": {}, \"packedBuffer\": false }",
				rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
		}
		else 
		{
			if (null != buf) {
				this.OnMsg (c, buf);
/* Received data equivalent to a heartbeat pong. */
				this.SetNextPong (this.last_activity.plusSeconds (c.pingTimeout()));
			}
			if (rc > 0)
			{
/* pending buffer needs flushing out before IO notification can resume */
				final SelectionKey key = c.selectableChannel().keyFor (selector);
				key.attach (Boolean.TRUE);
			}
		}
	}

	private boolean OnMsg (Channel c, TransportBuffer buf) {
		final DecodeIterator it = CodecFactory.createDecodeIterator();
		it.clear();
		final Msg msg = CodecFactory.createMsg();

/* Prepare codec */
		int rc = it.setBufferAndRWFVersion (buf, c.majorVersion(), c.minorVersion());
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.error ("DecodeIterator.setBufferAndRWFVersion: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
			return false;
		}

/* Decode data buffer into RSSL message */
		rc = msg.decode (it);
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.error ("Msg.decode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
			return false;
		} else {
			if (LOG.isDebugEnabled()) {
/* Pass through RSSL validation and report exceptions */
				if (!msg.validateMsg()) {
					LOG.warn ("Msg.ValidateMsg failed.");
					this.Abort (c);
					return false;
				} else {
					LOG.debug ("Msg.ValidateMsg success.");
				}
// FIXME: disable until serialization fixed for series dictionary
//				this.DecodeToXml (msg, buf, c.majorVersion(), c.minorVersion());
			}
			if (!this.OnMsg (c, it, msg))
				this.Abort (c);
			return true;
		}
	}

/* One can serialize from underlying transport buffer or inplace of .decode() operator.  This
 * function requires a buffer backed message, will not work when encoding a message or extracting
 * an encapsulated message.
 */
	private String DecodeToXml (Msg msg, int major_version, int minor_version) {
		return this.DecodeToXml (msg, msg.encodedMsgBuffer(), major_version, minor_version);
	}
	private String DecodeToXml (Msg msg, Buffer buf, int major_version, int minor_version) {
		final DecodeIterator it = CodecFactory.createDecodeIterator();
		it.clear();
		final int rc = it.setBufferAndRWFVersion (buf, major_version, minor_version);
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.warn ("DecodeIterator.setBufferAndRWFVersion: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
			return "";
		} else {
			return msg.decodeToXml (it, this.rdm_dictionary);
		}
	}
	private String DecodeToXml (Msg msg, TransportBuffer buf, int major_version, int minor_version) {
		final DecodeIterator it = CodecFactory.createDecodeIterator();
		it.clear();
		final int rc = it.setBufferAndRWFVersion (buf, major_version, minor_version);
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.warn ("DecodeIterator.setBufferAndRWFVersion: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
			return "";
		} else {
			return msg.decodeToXml (it, this.rdm_dictionary);
		}
	}

/* Create an item stream for a given symbol name.  The Item Stream maintains
 * the provider state on behalf of the application.
 */
	public void createItemStream (Instrument instrument, ItemStream item_stream) {
/* Construct directory unique key */
		this.sb.setLength (0);
		this.sb.append (instrument.getName());
		this.createItemStream (instrument, item_stream, this.sb.toString());
	}

	public void createItemStream (Instrument instrument, ItemStream item_stream, String key) {
		LOG.trace ("Creating item stream for RIC \"{}\" on service \"{}\".", instrument.getName(), this.config.getServiceName());

		if (!this.pending_dictionary) {
			item_stream.setToken (this.GenerateToken());
		}
		this.directory.put (key, item_stream);
		LOG.trace ("Directory size: {}", this.directory.size());
	}

	public boolean SendClose (Channel c, ItemStream item_stream) {
		StatusMsg msg = (StatusMsg)CodecFactory.createMsg();
		msg.domainType (item_stream.getMsgModelType());
		msg.msgClass (MsgClasses.STATUS);
		msg.streamId (item_stream.getToken());

		msg.msgKey().flags (MsgKeyFlags.HAS_SERVICE_ID | MsgKeyFlags.HAS_NAME_TYPE | MsgKeyFlags.HAS_NAME);
		msg.msgKey().serviceId (this.service_id);
		msg.msgKey().nameType (InstrumentNameTypes.RIC);
		msg.msgKey().name().data (item_stream.getItemName());
		msg.flags (RefreshMsgFlags.HAS_MSG_KEY);

		msg.state().streamState (StreamStates.CLOSED);
		msg.state().dataState (DataStates.NO_CHANGE);
		msg.state().code (StateCodes.NONE);

		final com.thomsonreuters.upa.transport.Error rssl_err = TransportFactory.createError();
		final TransportBuffer buf = c.getBuffer (MAX_MSG_SIZE, false /* not packed */, rssl_err);
		if (null == buf) {
			LOG.error ("Channel.getBuffer: { \"errorId\": {}, \"sysError\": \"{}\", \"text\": \"{}\", \"size\": {}, \"packedBuffer\": false }",
					rssl_err.errorId(), rssl_err.sysError(), rssl_err.text(),
					MAX_MSG_SIZE);
			return false;
		}
		final EncodeIterator it = CodecFactory.createEncodeIterator();
		it.clear();
		int rc = it.setBufferAndRWFVersion (buf, c.majorVersion(), c.minorVersion());
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.error ("EncodeIterator.setBufferAndRWFVersion: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"majorVersion\": {}, \"minorVersion\": {} }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc),
					c.majorVersion(), c.minorVersion());
			return false;
		}
		rc = msg.encodeInit (it, MAX_MSG_SIZE);
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.error ("Msg.encodeInit: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
			return false;
		}
		rc = msg.encodeComplete (it, true /* commit */);
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.error ("Msg.encodeComplete: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
			return false;
		}

		if (LOG.isDebugEnabled()) {
			final DecodeIterator jt = CodecFactory.createDecodeIterator();
			jt.clear();
			rc = jt.setBufferAndRWFVersion (buf, c.majorVersion(), c.minorVersion());
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.warn ("DecodeIterator.setBufferAndRWFVersion: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
			} else {
				LOG.debug ("{}", msg.decodeToXml (jt));
			}
		}

		if (LOG.isDebugEnabled()) {
                        final DecodeIterator jt = CodecFactory.createDecodeIterator();
                        jt.clear();
                        rc = jt.setBufferAndRWFVersion (buf, c.majorVersion(), c.minorVersion());
                        if (CodecReturnCodes.SUCCESS != rc) {
                                LOG.warn ("DecodeIterator.setBufferAndRWFVersion: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
                                        rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
                        } else {
                                LOG.debug ("{}", msg.decodeToXml (jt));
                        }
                }
/* Message validation. */
		if (!msg.validateMsg()) {
			LOG.error ("StatusMsg.validateMsg failed.");
			return false;
		}

		if (0 == this.Submit (c, buf)) {
			return false;
		} else {
			return true;
		}
	}

/* Returns true if message processed successfully, returns false to abort the connection.
 */
	public boolean OnMsg (Channel c, DecodeIterator it, Msg msg) {
		switch (msg.domainType()) {
		case DomainTypes.LOGIN:
			return this.OnLoginResponse (c, it, msg);
		case DomainTypes.DICTIONARY:
			return this.OnDictionary (c, it, msg);
		default:
			LOG.warn ("Uncaught message: {}", this.DecodeToXml (msg, c.majorVersion(), c.minorVersion()));
			return true;
		}
	}

	private boolean OnLoginResponse (Channel c, DecodeIterator it, Msg msg) {
		LOG.trace ("{}", this.DecodeToXml (msg, c.majorVersion(), c.minorVersion()));
		State state = null;

		switch (msg.msgClass()) {
		case MsgClasses.CLOSE:
			return this.OnLoginClosed (c, it, msg);

		case MsgClasses.REFRESH:
			state = ((RefreshMsg)msg).state();
			break;

		case MsgClasses.STATUS:
			state = ((StatusMsg)msg).state();
			break;

		default:
			LOG.warn ("Uncaught: {}", msg);
			return true;
		}

		assert (null != state);

/* extract out stream and data state like RFA */
		switch (state.streamState()) {
		case StreamStates.OPEN:
			switch (state.dataState()) {
			case DataStates.OK:
				return this.OnLoginSuccess (c, it, msg);
			case DataStates.SUSPECT:
				return this.OnLoginSuspect (c, it, msg);
			case DataStates.NO_CHANGE:
// by-definition, ignore
				return true;
			default:
				LOG.warn ("Uncaught data state: {}", msg);
				return true;
			}

		case StreamStates.CLOSED:
			return this.OnLoginClosed (c, it, msg);

		default:
			LOG.warn ("Uncaught stream state: {}", msg);
			return true;
		}
	}

	private boolean OnDictionary (Channel c, DecodeIterator it, Msg msg) {
		LOG.debug ("OnDictionary");

		if (MsgClasses.REFRESH != msg.msgClass()) {
/* Status can show a new dictionary but is not implemented in TREP-RT infrastructure, so ignore. */
/* Close should only happen when the infrastructure is in shutdown, defer to closed MMT_LOGIN. */
			LOG.warn ("Uncaught dictionary response message type: {}", MsgClasses.toString (msg.msgClass()));
			return true;
		}

		return this.OnDictionaryRefresh (c, it, (RefreshMsg)msg);
	}

/* Replace any existing RDM dictionary upon a dictionary refresh message.
 */
	private boolean OnDictionaryRefresh (Channel c, DecodeIterator it, RefreshMsg response) {
		final com.thomsonreuters.upa.transport.Error rssl_err = TransportFactory.createError();
		int rc;

		LOG.debug ("OnDictionaryRefresh");
		if (0 == (response.flags() & RefreshMsgFlags.HAS_MSG_KEY)) {
			LOG.warn ("Dictionary refresh messages should contain a msgKey component, rejecting.");
			return false;
		}
		final MsgKey msg_key = response.msgKey();

		switch (this.dictionary_tokens.inverse().get (response.streamId())) {
		case FIELD_DICTIONARY_NAME:
			rc = this.rdm_dictionary.decodeFieldDictionary (it, Dictionary.VerbosityValues.NORMAL, rssl_err);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.info ("DataDictionary.decodeFieldDictionary: { \"rsslErrorId\": {}, \"sysError\": {}, \"text\": \"{}\" }",
					rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
				return false;
			}
			break;

		case ENUM_TYPE_DICTIONARY_NAME:
			rc = this.rdm_dictionary.decodeEnumTypeDictionary (it, Dictionary.VerbosityValues.NORMAL, rssl_err);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.info ("DataDictionary.decodeEnumTypeDictionary: { \"rsslErrorId\": {}, \"sysError\": {}, \"text\": \"{}\" }",
					rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
				return false;
			}
			break;

/* Ignore unused dictionaries */
		default:
			return true;
		}

		if (0 != (response.flags() & RefreshMsgFlags.REFRESH_COMPLETE)
			&& 0 != this.rdm_dictionary.enumTableCount()
			&& 0 != this.rdm_dictionary.numberOfEntries())
		{
			LOG.info ("All used dictionaries loaded, sending directory.");
			this.pending_dictionary = false;
			if (!this.SendDirectoryResponse (c))
				return false;
			this.ResetTokens();
			LOG.info ("Unmuting provider.");
			this.is_muted = false;
			return this.Republish (c);
		}
		return true;
	}

	private boolean OnLoginSuccess (Channel c, DecodeIterator it, Msg msg) {
		LOG.debug ("OnLoginSuccess");
/* Log upstream application name, only presented in refresh messages. */
		switch (msg.msgClass()) {
		case MsgClasses.REFRESH:
			if (0 != (msg.flags() & RefreshMsgFlags.HAS_MSG_KEY)) {
				final MsgKey msg_key = msg.msgKey();
				if (0 != (msg_key.flags() & MsgKeyFlags.HAS_ATTRIB)) {
					int rc = msg.decodeKeyAttrib (it, msg_key);
					if (CodecReturnCodes.SUCCESS != rc) {
						LOG.warn ("Msg.decodeKeyAttrib: { \"returnCode\": {}, \"enumeration\": \"{}\" }",
							rc, CodecReturnCodes.toString (rc));
						return false;
					}
					final ElementList element_list = CodecFactory.createElementList();
					rc = element_list.decode (it, null /* local definitions */);
					if (CodecReturnCodes.SUCCESS != rc) {
						LOG.warn ("ElementList.decode: { \"returnCode\": {}, \"enumeration\": \"{}\" }",
							rc, CodecReturnCodes.toString (rc));
						return false;
					}
					final ElementEntry element_entry = CodecFactory.createElementEntry();
					for (;;) {
						rc = element_entry.decode (it);
						if (CodecReturnCodes.END_OF_CONTAINER == rc)
							break;
						if (CodecReturnCodes.SUCCESS != rc) {
							LOG.warn ("ElementEntry.decode: { \"returnCode\": {}, \"enumeration\": \"{}\" }",
								rc, CodecReturnCodes.toString (rc));
							return false;
						}
						if (element_entry.name().equals (ElementNames.APPNAME)) {
							if (DataTypes.ASCII_STRING != element_entry.dataType()) {
								LOG.warn ("Element entry APPNAME not expected data type ASCII_STRING.");
								return false;
							}
							final Buffer buffer = element_entry.encodedData();
							LOG.info ("applicationName: \"{}\"", buffer.toString());
							break;
						}
					}
				}
			}
		default:
			break;
		}
/* A new connection to TREP infrastructure, request dictionary to discover available services. */
		if (this.pending_dictionary) {
			if (!this.SendDictionaryRequest (c, this.service_id, FIELD_DICTIONARY_NAME))
				return false;
			if (!this.SendDictionaryRequest (c, this.service_id, ENUM_TYPE_DICTIONARY_NAME))
				return false;
		} else {
			if (!this.SendDirectoryResponse (c))
				return false;
			this.ResetTokens();
			LOG.info ("Unmuting provider.");
			this.is_muted = false;
			this.Republish (c);
		}
		return true;
	}

	private void ResetTokens() {
                LOG.debug ("Resetting {} provider tokens.", this.directory.size());
                for (ItemStream item_stream : this.directory.values()) {
                        item_stream.setToken (this.GenerateToken());
                }
        }

	private boolean OnLoginSuspect (Channel c, DecodeIterator it, Msg msg) {
		LOG.debug ("OnLoginSuspect");
		this.is_muted = true;
		return true;
	}

	private boolean OnLoginClosed (Channel c, DecodeIterator it, Msg msg) {
		LOG.debug ("OnLoginClosed");
		this.is_muted = true;
		return true;
	}

	public int Submit (Channel c, TransportBuffer buf) {
		final WriteArgs write_args = TransportFactory.createWriteArgs();
		final com.thomsonreuters.upa.transport.Error rssl_err = TransportFactory.createError();
		int rc;

		write_args.priority (WritePriorities.LOW);	/* flushing priority */
/* direct write on clear socket, enqueue when writes are pending */
		boolean should_write_direct = (0 == (c.selectableChannel().keyFor (this.selector).interestOps() & SelectionKey.OP_WRITE));
		write_args.flags (should_write_direct ? WriteFlags.DIRECT_SOCKET_WRITE : WriteFlags.NO_FLAGS);

		do {
			rc = c.write (buf, write_args, rssl_err);
			if (rc > 0) {
				LOG.debug ("Channel.write: { \"pendingBytes\": {}, \"bytesWritten\": {}, \"uncompressedBytesWritten\": {}, \"rsslErrorId\": {}, \"sysError\": {}, \"text\": \"{}\" }",
					rc,
					write_args.bytesWritten(), write_args.uncompressedBytesWritten(), rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
			} else {
				LOG.debug ("Channel.write: { \"returnCode\": {}, \"enumeration\": \"{}\", \"bytesWritten\": {}, \"uncompressedBytesWritten\": {}, \"rsslErrorId\": {}, \"sysError\": {}, \"text\": \"{}\" }",
					rc, TransportReturnCodes.toString (rc),
					write_args.bytesWritten(), write_args.uncompressedBytesWritten(), rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
			}

			if (rc > 0) {
				this.IncrementPendingCount();
			}
			if (rc > 0 
/* attempted to flush data to the connection but was blocked. */
				|| TransportReturnCodes.WRITE_FLUSH_FAILED == rc
/* empty buffer pool: spin wait until buffer is available. */
				|| TransportReturnCodes.NO_BUFFERS == rc)
			{
/* pending output */
				try {
					c.selectableChannel().keyFor (this.selector).interestOps (SelectionKey.OP_READ | SelectionKey.OP_WRITE);
				} catch (Exception e) {
					LOG.catching (e);
				}
				return -1;
			}
/* fragmenting the buffer and needs to be called again with the same buffer. */
		} while (TransportReturnCodes.WRITE_CALL_AGAIN == rc);
/* sent, no flush required. */
		if (TransportReturnCodes.SUCCESS != rc) {
			LOG.debug ("Channel.write: { \"rsslErrorId\": {}, \"sysError\": {}, \"text\": \"{}\" }",
				rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
			return 0;
		}
/* Sent data equivalent to a ping. */
		this.SetNextPing (this.last_activity.plusSeconds (this.ping_interval));
		return 1;
	}

	private boolean Ping (Channel c) {
		final com.thomsonreuters.upa.transport.Error rssl_err = TransportFactory.createError();

		final int rc = c.ping (rssl_err);
		if (rc > 0) {
			LOG.debug ("Channel.ping: { \"pendingBytes\": {}, \"rsslErrorId\": {}, \"sysError\": {}, \"text\": \"{}\" }",
				rc,
				rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
		} else {
			LOG.debug ("Channel.ping: { \"returnCode\": {}, \"enumeration\": \"{}, \"rsslErrorId\": {}, \"sysError\": {}, \"text\": \"{}\" }",
				rc, TransportReturnCodes.toString (rc),
				rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
		}
		if (TransportReturnCodes.WRITE_FLUSH_FAILED == rc
			|| TransportReturnCodes.NO_BUFFERS == rc
			|| rc > 0)
		{
			this.Abort (c);
			LOG.error ("Channel.ping: { \"errorId\": {}, \"sysError\": \"{}\", \"text\": \"{}\" }",
					rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
			return false;
		} else if (TransportReturnCodes.SUCCESS == rc) {	/* sent, no flush required. */
/* Advance ping expiration only on success. */
			this.SetNextPing (this.last_activity.plusSeconds (this.ping_interval));
			return true;
		} else {
			LOG.error ("Channel.ping: { \"errorId\": {}, \"sysError\": \"{}\", \"text\": \"{}\" }",
					rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
			return false;
		}
	}

/* Making a Login Request
 * A Login request message is encoded and sent by OMM Consumer and OMM non-
 * interactive provider applications.
 */
	private boolean SendLoginRequest (Channel c) {
		LOG.trace ("Sending login request.");
		final RequestMsg msg = (RequestMsg)CodecFactory.createMsg();
/* Set the message model type. */
		msg.domainType (DomainTypes.LOGIN);
/* Set request type. */
		msg.msgClass (MsgClasses.REQUEST);
		msg.flags (RequestMsgFlags.STREAMING);
/* No payload. */
		msg.containerType (DataTypes.NO_DATA);
/* Set the login token. */
		msg.streamId (this.login_token = this.GenerateToken());
LOG.debug ("login token {}", this.login_token);

/* DACS username (required). */
		msg.msgKey().nameType (Login.UserIdTypes.NAME);
		msg.msgKey().name().data (this.config.hasUserName() ?
						this.config.getUserName()
						: System.getProperty ("user.name"));
		msg.msgKey().flags (MsgKeyFlags.HAS_NAME_TYPE | MsgKeyFlags.HAS_NAME);

/* Login Request Elements */
		msg.msgKey().attribContainerType (DataTypes.ELEMENT_LIST);
		msg.msgKey().flags (msg.msgKey().flags() | MsgKeyFlags.HAS_ATTRIB);

		final com.thomsonreuters.upa.transport.Error rssl_err = TransportFactory.createError();
		final TransportBuffer buf = c.getBuffer (MAX_MSG_SIZE, false /* not packed */, rssl_err);
		if (null == buf) {
			LOG.error ("Channel.getBuffer: { \"errorId\": {}, \"sysError\": \"{}\", \"text\": \"{}\", \"size\": {}, \"packedBuffer\": false }",
					rssl_err.errorId(), rssl_err.sysError(), rssl_err.text(),
					MAX_MSG_SIZE);
			return false;
		}
		final EncodeIterator it = CodecFactory.createEncodeIterator();
		it.clear();
		int rc = it.setBufferAndRWFVersion (buf, c.majorVersion(), c.minorVersion());
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.error ("EncodeIterator.setBufferAndRWFVersion: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"majorVersion\": {}, \"minorVersion\": {} }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc),
					c.majorVersion(), c.minorVersion());
			return false;
		}
		rc = msg.encodeInit (it, MAX_MSG_SIZE);
		if (CodecReturnCodes.ENCODE_MSG_KEY_ATTRIB != rc) {
			LOG.error ("RequestMsg.encodeInit: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"dataMaxSize\": {} }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc), MAX_MSG_SIZE);
			return false;
		}

/* Encode attribute object after message instead of before as per RFA. */
		final ElementList element_list = CodecFactory.createElementList();
		final ElementEntry element_entry = CodecFactory.createElementEntry();
		final com.thomsonreuters.upa.codec.Buffer rssl_buffer = CodecFactory.createBuffer();
		final com.thomsonreuters.upa.codec.UInt rssl_uint = CodecFactory.createUInt();
		element_list.flags (ElementListFlags.HAS_STANDARD_DATA);
		rc = element_list.encodeInit (it, null /* element id dictionary */, 0 /* count of elements */);
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.error ("RequestMsg.encodeInit: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"flags\": \"HAS_STANDARD_DATA\" }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
			return false;
		}
/* DACS Application Id (optional).
 * e.g. "256"
 */
		if (this.config.hasApplicationId()) {
			rssl_buffer.data (this.config.getApplicationId());
			element_entry.dataType (DataTypes.ASCII_STRING);
			element_entry.name (ElementNames.APPID);
			rc = element_entry.encode (it, rssl_buffer);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("ElementEntry.encode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"name\": \"{}\", \"dataType\": \"{}\", \"applicationId\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc),
					element_entry.name(), DataTypes.toString (element_entry.dataType()), rssl_buffer);
				return false;
			}
		}
/* Instance Id (optional).
 * e.g. "<Instance Id>"
 */
		if (this.config.hasInstanceId()) {
			rssl_buffer.data (this.config.getInstanceId());
			element_entry.dataType (DataTypes.ASCII_STRING);
			element_entry.name (ElementNames.INST_ID);
			rc = element_entry.encode (it, rssl_buffer);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("ElementEntry.encode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"name\": \"{}\", \"dataType\": \"{}\", \"instanceId\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc),
					element_entry.name(), DataTypes.toString (element_entry.dataType()), rssl_buffer);
				return false;
			}
		}
/* DACS Position name (optional).
 * e.g. "127.0.0.1/net"
 */
		String position = null;
		if (this.config.hasPosition()) {
			if (!this.config.getPosition().isEmpty())
				position = this.config.getPosition();
			else
				position = "";
		} else {
			this.sb.setLength (0);
			try {
				this.sb .append (InetAddress.getLocalHost().getHostAddress())
					.append ('/')
					.append (InetAddress.getLocalHost().getHostName());
			} catch (UnknownHostException e) {
				LOG.catching (e);
				return false;
			}
			position = this.sb.toString();
		}
		rssl_buffer.data (position);
		element_entry.dataType (DataTypes.ASCII_STRING);
		element_entry.name (ElementNames.POSITION);
		rc = element_entry.encode (it, rssl_buffer);
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.error ("ElementEntry.encode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"name\": \"{}\", \"dataType\": \"{}\", \"position\": \"{}\" }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc),
				element_entry.name(), DataTypes.toString (element_entry.dataType()), rssl_buffer);
			return false;
		}
/* OMM non-interactive provider application role.
 */
		rssl_uint.value (Login.RoleTypes.PROV);
		element_entry.dataType (DataTypes.UINT);
		element_entry.name (ElementNames.ROLE);
		rc = element_entry.encode (it, rssl_uint);
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.error ("ElementEntry.encode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"name\": \"{}\", \"dataType\": \"{}\", \"role\": \"{}\" }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc),
				element_entry.name(), DataTypes.toString (element_entry.dataType()), rssl_uint);
			return false;
		}
		
		rc = element_list.encodeComplete (it, true /* commit */);
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.error ("ElementList.encodeComplete: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
			return false;
		}
		rc = msg.encodeKeyAttribComplete (it, true /* commit */);
		if (CodecReturnCodes.ENCODE_CONTAINER != rc) {
			LOG.error ("RequestMsg.encodeKeyAttribComplete: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
			return false;
		}
		rc = msg.encodeComplete (it, true /* commit */);
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.error ("RequestMsg.encodeComplete: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
			return false;
		}

		if (LOG.isDebugEnabled()) {
                        final DecodeIterator jt = CodecFactory.createDecodeIterator();
                        jt.clear();
                        rc = jt.setBufferAndRWFVersion (buf, c.majorVersion(), c.minorVersion());
                        if (CodecReturnCodes.SUCCESS != rc) {
                                LOG.warn ("DecodeIterator.setBufferAndRWFVersion: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
                                        rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
                        } else {
                                LOG.debug ("{}", msg.decodeToXml (jt));
                        }
                }
/* Message validation. */
		if (!msg.validateMsg()) {
			LOG.error ("RequestMsg.validateMsg failed.");
			return false;
		}

		if (0 == this.Submit (c, buf)) {
			return false;
		} else {
			return true;
		}
	}

	private boolean SendDirectoryResponse (Channel c) {
		LOG.trace ("Sending directory response.");
		final RefreshMsg msg = (RefreshMsg)CodecFactory.createMsg();
		msg.domainType (DomainTypes.SOURCE);
		msg.msgClass (MsgClasses.REFRESH);
		msg.flags (RefreshMsgFlags.REFRESH_COMPLETE | RefreshMsgFlags.HAS_MSG_KEY);
		msg.streamId (this.directory_token = this.GenerateToken());
		msg.containerType (DataTypes.MAP);

		msg.msgKey().flags (MsgKeyFlags.HAS_FILTER);
		msg.msgKey().filter (Directory.ServiceFilterIds.INFO | Directory.ServiceFilterIds.STATE);

		msg.state().streamState (StreamStates.OPEN);
		msg.state().dataState (DataStates.OK);
		msg.state().code (StateCodes.NONE);

		final com.thomsonreuters.upa.transport.Error rssl_err = TransportFactory.createError();
		final TransportBuffer buf = c.getBuffer (MAX_MSG_SIZE, false /* not packed */, rssl_err);
		if (null == buf) {
			LOG.error ("Channel.getBuffer: { \"errorId\": {}, \"sysError\": \"{}\", \"text\": \"{}\", \"size\": {}, \"packedBuffer\": false }",
					rssl_err.errorId(), rssl_err.sysError(), rssl_err.text(),
					MAX_MSG_SIZE);
			return false;
		}
		final EncodeIterator it = CodecFactory.createEncodeIterator();
		it.clear();
		int rc = it.setBufferAndRWFVersion (buf, c.majorVersion(), c.minorVersion());
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.error ("EncodeIterator.setBufferAndRWFVersion: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"majorVersion\": {}, \"minorVersion\": {} }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc),
					c.majorVersion(), c.minorVersion());
			return false;
		}
		rc = msg.encodeInit (it, MAX_MSG_SIZE);
		if (CodecReturnCodes.ENCODE_CONTAINER != rc) {
			LOG.error ("Msg.encodeInit: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
			return false;
		}
/* A source directory is a RSSL map of UINT service id -> FILTER_LIST */
		final com.thomsonreuters.upa.codec.Map map = CodecFactory.createMap();
		map.clear();
		map.keyPrimitiveType (DataTypes.UINT);
		map.containerType (DataTypes.FILTER_LIST);
		rc = map.encodeInit (it, 0, 0);
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.error ("Map.encodeInit: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
			return false;
		}
		{
/* for each service */
			MapEntry map_entry = CodecFactory.createMapEntry();
			map_entry.clear();
			map_entry.action (MapEntryActions.ADD);
			com.thomsonreuters.upa.codec.UInt rssl_uint = CodecFactory.createUInt();
			rssl_uint.value (this.service_id);
			rc = map_entry.encodeInit (it, rssl_uint, 0);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("MapEntry.encodeInit: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
				return false;
			}
/* A service is represented by a RSSL filter list of FILTER_ENTRY = ELEMENT_LIST */
			FilterList filter_list = CodecFactory.createFilterList();
			filter_list.clear();
			filter_list.containerType (DataTypes.ELEMENT_LIST);
			rc = filter_list.encodeInit (it);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("FilterList.encodeInit: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
				return false;
			}
/* Filter:  INFO */
			FilterEntry filter_entry = CodecFactory.createFilterEntry();
			filter_entry.clear();
			filter_entry.id (Directory.ServiceFilterIds.INFO);
			filter_entry.action (FilterEntryActions.SET);
			filter_entry.containerType (DataTypes.ELEMENT_LIST);
			filter_entry.flags (FilterEntryFlags.HAS_CONTAINER_TYPE);
			rc = filter_entry.encodeInit (it, 0);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("FilterEntry.encodeInit: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
				return false;
			}
			ElementList element_list = CodecFactory.createElementList();
			element_list.clear();
			element_list.flags (ElementListFlags.HAS_STANDARD_DATA);
			rc = element_list.encodeInit (it, null, 0);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("ElementList.encodeInit: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
				return false;
			}
			ElementEntry element_entry = CodecFactory.createElementEntry();
/* service name */
			element_entry.clear();
			element_entry.name (ElementNames.NAME);
			element_entry.dataType (DataTypes.ASCII_STRING);
			com.thomsonreuters.upa.codec.Buffer rssl_buffer = CodecFactory.createBuffer();
			rssl_buffer.clear();
			rssl_buffer.data (this.config.getServiceName());
			rc = element_entry.encode (it, rssl_buffer);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("ElementEntry.encode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
				return false;
			}
/* service capabilities, i.e. supported domains */
			element_entry.clear();
			element_entry.name (ElementNames.CAPABILITIES);
			element_entry.dataType (DataTypes.ARRAY);
			rc = element_entry.encodeInit (it, 0);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("ElementEntry.encodeInit: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
				return false;
			}
			com.thomsonreuters.upa.codec.Array array = CodecFactory.createArray();
			array.clear();
			array.primitiveType (DataTypes.UINT);
			rc = array.encodeInit (it);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("Array.encodeInit: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
				return false;
			}
			ArrayEntry array_entry = CodecFactory.createArrayEntry();
/* domain: symbol list */
			array_entry.clear();
			rssl_uint.clear();
			rssl_uint.value (DomainTypes.SYMBOL_LIST);
			rc = array_entry.encode (it, rssl_uint);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("ArrayEntry.encode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
				return false;
			}
			rc = array.encodeComplete (it, true /* commit */);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("ArrayEntry.encodeComplete: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
				return false;
			}
			rc = element_entry.encodeComplete (it, true /* commit */);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("ElementEntry.encodeComplete: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
				return false;
			}
/* service dictionaries used */
			element_entry.clear();
			element_entry.name (ElementNames.DICTIONARIES_USED);
			element_entry.dataType (DataTypes.ARRAY);
                        rc = element_entry.encodeInit (it, 0);
                        if (CodecReturnCodes.SUCCESS != rc) {
                                LOG.error ("ElementEntry.encodeInit: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
                                        rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
                                return false;
                        }
			array.clear();
                        array.primitiveType (DataTypes.ASCII_STRING);
			rc = array.encodeInit (it);
                        if (CodecReturnCodes.SUCCESS != rc) {
                                LOG.error ("Array.encodeInit: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
                                        rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
                                return false;
                        }
/* standard TREP dictionaries */
			array_entry.clear();
			rssl_buffer.clear();
			rssl_buffer.data (FIELD_DICTIONARY_NAME);
			rc = array_entry.encode (it, rssl_buffer);
			if (CodecReturnCodes.SUCCESS != rc) {
                                LOG.error ("ArrayEntry.encode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }", 
                                        rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
                                return false;
                        }
			rssl_buffer.clear();
			rssl_buffer.data (ENUM_TYPE_DICTIONARY_NAME);
			rc = array_entry.encode (it, rssl_buffer);
                        if (CodecReturnCodes.SUCCESS != rc) {
                                LOG.error ("ArrayEntry.encode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
                                        rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
                                return false;
                        }
                        rc = array.encodeComplete (it, true /* commit */);
                        if (CodecReturnCodes.SUCCESS != rc) {
                                LOG.error ("ArrayEntry.encodeComplete: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
                                        rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
                                return false;
                        }
                        rc = element_entry.encodeComplete (it, true /* commit */);
                        if (CodecReturnCodes.SUCCESS != rc) {
                                LOG.error ("ElementEntry.encodeComplete: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
                                        rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
                                return false;
                        }
	
			rc = element_list.encodeComplete (it, true /* commit */);
                        if (CodecReturnCodes.SUCCESS != rc) {
                                LOG.error ("ElementList.encodeComplete: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
                                        rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
                                return false;
                        }
			rc = filter_entry.encodeComplete (it, true /* commit */);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("FilterEntry.encodeComplete: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
				return false;
			}

/* Filter:  STATE */
                        filter_entry.clear();
			filter_entry.id (Directory.ServiceFilterIds.STATE);
                        filter_entry.action (FilterEntryActions.SET);
			filter_entry.containerType (DataTypes.ELEMENT_LIST);
			filter_entry.flags (FilterEntryFlags.HAS_CONTAINER_TYPE);
                        rc = filter_entry.encodeInit (it, 0);
                        if (CodecReturnCodes.SUCCESS != rc) {
                                LOG.error ("FilterEntry.encodeInit: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
                                        rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
                                return false;
                        }
                        element_list.clear();
			element_list.flags (ElementListFlags.HAS_STANDARD_DATA);
                        rc = element_list.encodeInit (it, null, 0);
                        if (CodecReturnCodes.SUCCESS != rc) {
                                LOG.error ("ElementList.encodeInit: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
                                        rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
                                return false;
                        }
			element_entry.clear();
			element_entry.name (ElementNames.SVC_STATE);
			element_entry.dataType (DataTypes.UINT);
			rssl_uint.clear();
			rssl_uint.value (Directory.ServiceStates.UP);
			rc = element_entry.encode (it, rssl_uint);
                        if (CodecReturnCodes.SUCCESS != rc) {
                                LOG.error ("ElementEntry.encode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
                                        rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
                                return false;
                        }

			rc = element_list.encodeComplete (it, true /* commit */);
                        if (CodecReturnCodes.SUCCESS != rc) {
                                LOG.error ("ElementList.encodeComplete: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
                                        rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
                                return false;
                        }
			rc = filter_entry.encodeComplete (it, true /* commit */);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("FilterEntry.encodeComplete: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
				return false;
			}
			rc = filter_list.encodeComplete (it, true /* commit */);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("FilterList.encodeComplete: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
				return false;
			}

			rc = map_entry.encodeComplete (it, true /* commit */);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("MapEntry.encodeComplete: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
				return false;
			}
		}
		rc = map.encodeComplete (it, true /* commit */);
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.error ("Map.encodeComplete: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
			return false;
		}
		rc = msg.encodeComplete (it, true /* commit */);
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.error ("Msg.encodeComplete: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
			return false;
		}

		if (LOG.isDebugEnabled()) {
                        final DecodeIterator jt = CodecFactory.createDecodeIterator();
                        jt.clear();
                        rc = jt.setBufferAndRWFVersion (buf, c.majorVersion(), c.minorVersion());
                        if (CodecReturnCodes.SUCCESS != rc) {
                                LOG.warn ("DecodeIterator.setBufferAndRWFVersion: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
                                        rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
                        } else {
                                LOG.debug ("{}", msg.decodeToXml (jt));
                        }
                }
/* Message validation. */
		if (!msg.validateMsg()) {
			LOG.error ("RefreshMsg.validateMsg failed.");
			return false;
		}
		if (0 == this.Submit (c, buf)) {
                        return false;
                } else {
			return true;
		}
	}

	private boolean SendDictionaryRequest (Channel c, int service_id, String dictionary_name) {
		LOG.trace ("Sending dictionary request for \"{}\" from service #{}.", dictionary_name, service_id);
		final RequestMsg msg = (RequestMsg)CodecFactory.createMsg();
/* Set the message model type. */
		msg.domainType (DomainTypes.DICTIONARY);
/* Set request type. */
		msg.msgClass (MsgClasses.REQUEST);
		msg.flags (RequestMsgFlags.NONE);
/* No payload. */
		msg.containerType (DataTypes.NO_DATA);
/* Set the dictionary token. */
		msg.streamId (this.token);
LOG.debug ("dictionary token {}", this.token);

/* In RFA lingo an attribute object. */
		msg.msgKey().serviceId (service_id);
		final Buffer rssl_buf = CodecFactory.createBuffer();
		rssl_buf.data (dictionary_name);
		msg.msgKey().name (rssl_buf);
// RDMDictionary.Filter.NORMAL=0x7: Provides all information needed for decoding
		msg.msgKey().filter (Dictionary.VerbosityValues.NORMAL);
		msg.msgKey().flags (MsgKeyFlags.HAS_SERVICE_ID | MsgKeyFlags.HAS_NAME | MsgKeyFlags.HAS_FILTER);

		final com.thomsonreuters.upa.transport.Error rssl_err = TransportFactory.createError();
		final TransportBuffer buf = c.getBuffer (MAX_MSG_SIZE, false /* not packed */, rssl_err);
		if (null == buf) {
			LOG.error ("Channel.getBuffer: { \"errorId\": {}, \"sysError\": \"{}\", \"text\": \"{}\", \"size\": {}, \"packedBuffer\": false }",
					rssl_err.errorId(), rssl_err.sysError(), rssl_err.text(),
					MAX_MSG_SIZE);
			return false;
		}
		final EncodeIterator it = CodecFactory.createEncodeIterator();
		it.clear();
		int rc = it.setBufferAndRWFVersion (buf, c.majorVersion(), c.minorVersion());
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.error ("EncodeIterator.setBufferAndRWFVersion: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"majorVersion\": {}, \"minorVersion\": {} }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc),
					c.majorVersion(), c.minorVersion());
			return false;
		}
		rc = msg.encode (it);
		if (CodecReturnCodes.SUCCESS != rc) {
			LOG.error ("RequestMsg.encode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
			return false;
		}

		if (LOG.isDebugEnabled()) {
                        final DecodeIterator jt = CodecFactory.createDecodeIterator();
                        jt.clear();
                        rc = jt.setBufferAndRWFVersion (buf, c.majorVersion(), c.minorVersion());
                        if (CodecReturnCodes.SUCCESS != rc) {
                                LOG.warn ("DecodeIterator.setBufferAndRWFVersion: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
                                        rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
                        } else {
                                LOG.debug ("{}", msg.decodeToXml (jt));
                        }
                }
/* Message validation. */
		if (!msg.validateMsg()) {
			LOG.error ("RequestMsg.validateMsg failed.");
			return false;
		}

		if (0 == this.Submit (c, buf)) {
			return false;
		} else {
/* re-use token on failure. */
			this.dictionary_tokens.put (dictionary_name, this.GenerateToken());
			return true;
		}
	}

	private boolean Republish (Channel c) {
		for (ItemStream item_stream : this.directory.values()) {
			final com.thomsonreuters.upa.transport.Error rssl_err = TransportFactory.createError();
			final TransportBuffer buf = c.getBuffer (MAX_MSG_SIZE, false /* not packed */, rssl_err);
			if (null == buf) {
				LOG.error ("Channel.getBuffer: { \"errorId\": {}, \"sysError\": \"{}\", \"text\": \"{}\", \"size\": {}, \"packedBuffer\": false }",
						rssl_err.errorId(), rssl_err.sysError(), rssl_err.text(),
						MAX_MSG_SIZE);
				return false;
			}
			final EncodeIterator it = CodecFactory.createEncodeIterator();
			it.clear();
			int rc = it.setBufferAndRWFVersion (buf, c.majorVersion(), c.minorVersion());
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("EncodeIterator.setBufferAndRWFVersion: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\", \"majorVersion\": {}, \"minorVersion\": {} }",
						rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc),
						c.majorVersion(), c.minorVersion());
				return false;
			}

			if (!this.delegate.OnRequest (c, this.service_id, item_stream, it, buf))
				return false;
		}
		return true;
	}

}

/* eof */
