/* UPA based broadcast publisher.
 */

import java.io.*;
import java.util.*;
import java.net.*;
/* workaround pre-Java 9 */
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
/* end-workaround */
import org.apache.commons.cli.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.slf4j.bridge.SLF4JBridgeHandler;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.net.InetAddresses;
import com.google.common.primitives.UnsignedInts;
import com.thomsonreuters.upa.codec.Buffer;
import com.thomsonreuters.upa.codec.CodecFactory;
import com.thomsonreuters.upa.codec.CodecReturnCodes;
import com.thomsonreuters.upa.codec.DataStates;
import com.thomsonreuters.upa.codec.DataTypes;
import com.thomsonreuters.upa.codec.DecodeIterator;
import com.thomsonreuters.upa.codec.EncodeIterator;
import com.thomsonreuters.upa.codec.FieldEntry;
import com.thomsonreuters.upa.codec.FieldList;
import com.thomsonreuters.upa.codec.FieldListFlags;
import com.thomsonreuters.upa.codec.MapEntryActions;
import com.thomsonreuters.upa.codec.MapFlags;
import com.thomsonreuters.upa.codec.Msg;
import com.thomsonreuters.upa.codec.MsgClasses;
import com.thomsonreuters.upa.codec.MsgKeyFlags;
import com.thomsonreuters.upa.codec.RefreshMsg;
import com.thomsonreuters.upa.codec.RefreshMsgFlags;
import com.thomsonreuters.upa.codec.StateCodes;
import com.thomsonreuters.upa.codec.StreamStates;
import com.thomsonreuters.upa.rdm.InstrumentNameTypes;
import com.thomsonreuters.upa.transport.Channel;
import com.thomsonreuters.upa.transport.TransportBuffer;


public class Shinboru implements Provider.Delegate {

/* Application configuration. */
	private Config config;

/* UPA context. */
	private Upa upa;

/* UPA provider */
	private Provider provider;

/* Identifier for this running application instance, cannot use PostUserInfo as it is an interface. */
	private long publisher_address;
	private long publisher_id;

/* Instrument list. */
	private List<ItemStream> streams;

	private static Logger LOG = LogManager.getLogger (Shinboru.class.getName());

	private static final String RSSL_PROTOCOL		= "rssl";

/* RDM Usage Guide: Section 6.5: Enterprise Platform
 * For future compatibility, the DictionaryId should be set to 1 by providers.
 * The DictionaryId for the RDMFieldDictionary is 1.
 */
	private static final short  DICTIONARY_ID		= 1;
	private static final short  FIELD_LIST_ID		= 3;

	private static final int MAX_MSG_SIZE			= 4096;

	private static final boolean USE_DATA_DEFINITIONS	= true;

	private static final String SERVER_LIST_PARAM		= "server-list";
	private static final String APPLICATION_ID_PARAM	= "application-id";
	private static final String INSTANCE_ID_PARAM		= "instance-id";
	private static final String POSITION_PARAM		= "position";
	private static final String DICTIONARY_PARAM		= "dictionary";

	private static final String SESSION_OPTION		= "session";
	private static final String SYMBOL_PATH_OPTION		= "symbol-path";
	private static final String SYMBOL_LIST_OPTION		= "symbol-list";
	private static final String HELP_OPTION			= "help";
	private static final String VERSION_OPTION		= "version";

	private static final String SESSION_NAME		= "Session";
	private static final String CONNECTION_NAME		= "Connection";
	private static final String PROVIDER_NAME		= "Provider";

	private static Options buildOptions() {
		Options opts = new Options();

		Option help = OptionBuilder.withLongOpt (HELP_OPTION)
					.withDescription ("print this message")
					.create ("h");
		opts.addOption (help);

		Option version = OptionBuilder.withLongOpt (VERSION_OPTION)
					.withDescription ("print version information and exit")
					.create();
		opts.addOption (version);

		Option session = OptionBuilder.hasArg()
					.isRequired()
					.withArgName ("uri")
					.withDescription ("TREP-RT session declaration")
					.withLongOpt (SESSION_OPTION)
					.create();
		opts.addOption (session);

		Option symbol_path = OptionBuilder.hasArg()
					.isRequired()
					.withArgName ("file")
					.withDescription ("read from symbol path")
					.withLongOpt (SYMBOL_PATH_OPTION)
					.create();
		opts.addOption (symbol_path);

		Option symbol_list = OptionBuilder.hasArg()
					.isRequired()
					.withArgName ("ric")
					.withDescription ("publish to symbol list")
					.withLongOpt (SYMBOL_LIST_OPTION)
					.create();
		opts.addOption (symbol_list);

		return opts;
	}

	private static void printHelp (Options options) {
		new HelpFormatter().printHelp ("Shinboru", options);
	}

	private static Map<String, String> parseQuery (String query) throws UnsupportedEncodingException {
		final Map<String, String> query_pairs = new LinkedHashMap<String, String>();
		if (!Strings.isNullOrEmpty (query)) {
			final String[] pairs = query.split ("&");
			for (String pair : pairs) {
				int idx = pair.indexOf ("=");
				query_pairs.put (URLDecoder.decode (pair.substring (0, idx), "UTF-8"),
					URLDecoder.decode (pair.substring (idx + 1), "UTF-8"));
			}
		}
		return query_pairs;
	}

	private boolean Initialize (CommandLine line, Options options) throws Exception {
		if (line.hasOption (HELP_OPTION)) {
			printHelp (options);
			return false;
		}

/* Configuration. */
		this.config = new Config();

		if (line.hasOption (SESSION_OPTION)) {
			final String session = line.getOptionValue (SESSION_OPTION);
			List<SessionConfig> session_configs = new ArrayList<SessionConfig>();
			if (!Strings.isNullOrEmpty (session)) {
				LOG.info ("Session declaration: {}", session);
				final URI parsed = new URI (session);
/* For each key-value pair, i.e. ?a=x&b=y&c=z -> (a,x) (b,y) (c,z) */
				final ImmutableMap<String, String> query = ImmutableMap.copyOf (parseQuery (parsed.getQuery()));

/* Extract out required parameters */
				final String protocol = parsed.getScheme();
				final String server_list = query.get (SERVER_LIST_PARAM);
				String[] servers = { parsed.getHost() };
/* Override host in URL with server-list query parameter */
				if (!Strings.isNullOrEmpty (server_list)) {
					servers = Iterables.toArray (Splitter.on (',')
							.trimResults()
							.omitEmptyStrings()
							.split (server_list), String.class);
				}
				String service_name = null;
/* Catch default URL of host/ as empty */
				if (!Strings.isNullOrEmpty (parsed.getPath())
					&& parsed.getPath().length() > 1)
				{
					service_name = new File (parsed.getPath()).getName();
				}

/* Minimum parameters to construct session configuration */
				SessionConfig session_config = new SessionConfig (SESSION_NAME, CONNECTION_NAME, PROVIDER_NAME, protocol, servers, service_name);

/* Optional session parameters: */
				if (!Strings.isNullOrEmpty (parsed.getUserInfo()))
					session_config.setUserName (parsed.getUserInfo());
/* -1 if the port is undefined */
				if (-1 != parsed.getPort()) 
					session_config.setDefaultPort (Integer.toString (parsed.getPort()));
				if (query.containsKey (APPLICATION_ID_PARAM))
					session_config.setApplicationId (query.get (APPLICATION_ID_PARAM));
				if (query.containsKey (INSTANCE_ID_PARAM))
					session_config.setInstanceId (query.get (INSTANCE_ID_PARAM));
				if (query.containsKey (POSITION_PARAM))
					session_config.setPosition (query.get (POSITION_PARAM));
				if (query.containsKey (DICTIONARY_PARAM)) {
					Iterator<String> it = Splitter.on (',')
									.trimResults()
									.limit (2)
									.split (query.get (DICTIONARY_PARAM)).iterator();
					if (it.hasNext())
						session_config.setFieldDictionary (it.next());
					if (it.hasNext())
						session_config.setEnumDictionary (it.next());
				}

				LOG.trace ("Session evaluation: {}", session_config.toString());
				session_configs.add (session_config);
			}
			if (!session_configs.isEmpty()) {
				final SessionConfig[] array = session_configs.toArray (new SessionConfig[session_configs.size()]);
				this.config.setSessions (array);
			}
		}

/* Publish list. */
		List<Instrument> instruments = new ArrayList<Instrument> ();
/* Symbol list. */
		List<String> symbols = new ArrayList<String> ();
		if (line.hasOption (SYMBOL_PATH_OPTION)) {
			this.config.setSymbolPath (line.getOptionValue (SYMBOL_PATH_OPTION));
			File symbol_path = new File (this.config.getSymbolPath());
			if (symbol_path.canRead()) {
				Scanner line_scanner = new Scanner (symbol_path);
				int line_number = 0;	// for error notices
				try {
					while (line_scanner.hasNextLine()) {
						++line_number;
						Scanner field_scanner = new Scanner (line_scanner.nextLine());
						field_scanner.useDelimiter (",");
						if (!field_scanner.hasNext())
							throw new IOException ("Missing symbol name field in symbol file \"" + this.config.getSymbolPath() + "\" line " + line_number + ".");
						String symbol_name = field_scanner.next();
						symbols.add (symbol_name);
					}
				} finally {
					line_scanner.close();
				}
				LOG.info ("Read {} symbols from {}.", symbols.size(), symbol_path);
			}
		}

		if (line.hasOption (SYMBOL_LIST_OPTION)) {
			String symbol_list_name = line.getOptionValue (SYMBOL_LIST_OPTION);
			Instrument instrument = new Instrument (symbol_list_name, symbols);
			instruments.add (instrument);
		}

		LOG.debug (this.config.toString());

/* UPA Context. */
		this.upa = new Upa (this.config);
		if (!this.upa.Initialize()) {
			return false;
		}

/* UPA provider */
		this.provider = new Provider (this.config.getSession(),
					this.upa,
					this /* Provider.delegate */);
		if (!this.provider.Initialize()) {
			return false;
		}

/* Define this running instance identity. */
		this.publisher_address = UnsignedInts.toLong (InetAddresses.coerceToInteger (InetAddress.getLocalHost()));
		{
/* pre-Java 9: ProcessHandle.current().getPid() */
			RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
			String jvmName = runtimeBean.getName();
			long pid = Long.valueOf (jvmName.split("@")[0]);
			this.publisher_id = pid;
		}
		LOG.info ("Publisher identity: {\"address\": {}, \"id\": {}}", this.publisher_address, this.publisher_id);

/* Create state for published RIC. */
		this.streams = new ArrayList<ItemStream> (instruments.size());
		for (Instrument instrument : instruments) {
			ItemStream stream = new SymbolListStream (instrument.getName(), instrument.getSymbols());
			this.provider.createItemStream (instrument, stream);
			this.streams.add (stream);
			LOG.trace (instrument.toString());
		}

		return true;
	}

/* LOG4J2 logging is terminated by an installed shutdown hook.	This hook can
 * disabled by adding shutdownHook="disable" to the <Configuration> stanza.
 */
	private class ShutdownThread extends Thread {
		private Shinboru app;
		private org.apache.logging.log4j.core.LoggerContext context;
		public ShutdownThread (Shinboru app) {
			this.app = app;
/* Capture on startup as we cannot capture on shutdown as it would try to reinit:
 *   WARN Unable to register shutdown hook due to JVM state
 */
			this.context = (org.apache.logging.log4j.core.LoggerContext)LogManager.getContext();
		}
		@Override
		public void run() {
			setName ("shutdown");
/* TBD: exit mainloop */
/* LOG4J2-318 to manually shutdown.
 */
			if (context.isStarted()
				&& !context.getConfiguration().isShutdownHookEnabled())
			{
				LOG.trace ("Shutdown log4j2.");
				context.stop();
			}
		}
	}

	private void run (CommandLine line, Options options) throws Exception {
		this.Initialize (line, options);
		Thread shutdown_hook = new ShutdownThread (this);
		Runtime.getRuntime().addShutdownHook (shutdown_hook);
		LOG.trace ("Shutdown hook installed.");
		this.mainloop();
		LOG.trace ("Shutdown in progress.");
/* Cannot remove hook if shutdown is in progress. */
//		Runtime.getRuntime().removeShutdownHook (shutdown_hook);
//		LOG.trace ("Removed shutdown hook.");
		this.Close();
		this.is_shutdown = true;
	}

	public volatile boolean is_shutdown = false;

	private void mainloop() {
		LOG.trace ("Waiting ...");
		this.provider.Run();
		LOG.trace ("Mainloop deactivated.");
	}

/* Publish response for registered item stream. */
	@Override
	public boolean OnRequest (Channel c, int service_id, ItemStream stream, EncodeIterator it, TransportBuffer buf) {
		SymbolListStream symbol_list_stream = (SymbolListStream)stream;
		final RefreshMsg msg = (RefreshMsg)CodecFactory.createMsg();

		msg.msgClass (MsgClasses.REFRESH);
		msg.flags (RefreshMsgFlags.HAS_MSG_KEY | RefreshMsgFlags.REFRESH_COMPLETE | RefreshMsgFlags.HAS_POST_USER_INFO);
/* Set the message model type. */
		msg.domainType (stream.getMsgModelType());
/* Set the item stream token. */
		msg.streamId (stream.getToken());
		msg.containerType (DataTypes.MAP);

/* In RFA lingo an attribute object. */
		msg.msgKey().flags (MsgKeyFlags.HAS_SERVICE_ID | MsgKeyFlags.HAS_NAME_TYPE | MsgKeyFlags.HAS_NAME);
		msg.msgKey().serviceId (service_id);
		msg.msgKey().nameType (InstrumentNameTypes.RIC);
		final Buffer rssl_buffer = CodecFactory.createBuffer();
		rssl_buffer.data (stream.getItemName());
		msg.msgKey().name (rssl_buffer);

		msg.postUserInfo().userAddr (this.publisher_address);
		msg.postUserInfo().userId (this.publisher_id);

		msg.state().streamState (StreamStates.OPEN);
		msg.state().dataState (DataStates.OK);
		msg.state().code (StateCodes.NONE);

		int rc = msg.encodeInit (it, MAX_MSG_SIZE);
		if (CodecReturnCodes.ENCODE_CONTAINER != rc) {
			LOG.error ("Msg.encodeInit: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
				rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
			return false;
		}

		{
			final com.thomsonreuters.upa.codec.Map map = CodecFactory.createMap();
			map.clear();
			map.flags (MapFlags.HAS_TOTAL_COUNT_HINT);
			map.totalCountHint (symbol_list_stream.getSymbols().size());
			map.keyPrimitiveType (DataTypes.BUFFER);
			map.containerType (DataTypes.FIELD_LIST);
			rc = map.encodeInit (it, 0, 0);
			if (CodecReturnCodes.SUCCESS != rc) {
				LOG.error ("Map.encodeInit: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
					rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
				return false;
			}

			final com.thomsonreuters.upa.codec.MapEntry map_entry = CodecFactory.createMapEntry();
			map_entry.clear();
			map_entry.action (MapEntryActions.ADD);
			FieldList field_list = CodecFactory.createFieldList();
			FieldEntry field_entry = CodecFactory.createFieldEntry();
			
			for (String symbol : symbol_list_stream.getSymbols()) {
				rssl_buffer.data (symbol);
				rc = map_entry.encodeInit (it, rssl_buffer, 0 /* max size */);
				if (CodecReturnCodes.SUCCESS != rc) {
					LOG.error ("MapEntry.encodeInit: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
						rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
					return false;
				}

				field_list.clear();
				field_list.flags (FieldListFlags.HAS_STANDARD_DATA | FieldListFlags.HAS_FIELD_LIST_INFO);
				field_list.dictionaryId (DICTIONARY_ID);
				field_list.fieldListNum (FIELD_LIST_ID);
				rc = field_list.encodeInit (it, null /* dictionary */, 0 /* max size */);
				if (CodecReturnCodes.SUCCESS != rc) {
					LOG.error ("FieldList.encodeInit: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
						rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
					return false;
				}

				field_entry.clear();
				field_entry.fieldId (3422);
				field_entry.dataType (DataTypes.RMTES_STRING);
				rc = field_entry.encode (it, rssl_buffer);
				if (CodecReturnCodes.SUCCESS != rc) {
					LOG.error ("FieldEntry.encode: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
						rc, CodecReturnCodes.toString (rc), CodecReturnCodes.info (rc));
					return false;
				}

				rc = field_list.encodeComplete (it, true /* commit */);
				if (CodecReturnCodes.SUCCESS != rc) {
					LOG.error ("FieldList.encodeComplete: { \"returnCode\": {}, \"enumeration\": \"{}\", \"text\": \"{}\" }",
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
			LOG.error ("Msg.validateMsg failed.");
			return false;
		}

		if (0 == this.provider.Submit (c, buf)) {
			return false;
		} else {
			return true;
		}
	}

	private void Close() {
		if (null != this.provider) {
			LOG.trace ("Closing Provider.");
			this.provider.Close();
			this.provider = null;
		}

		if (null != this.upa) {
			LOG.trace ("Closing UPA.");
			this.upa.Close();
			this.upa = null;
		}
	}

	public static void main (String[] args) throws Exception {
		final Options options = Shinboru.buildOptions();
		final CommandLine line = new PosixParser().parse (options, args);
		Shinboru app = new Shinboru();
		app.run (line, options);
	}
}

/* eof */
