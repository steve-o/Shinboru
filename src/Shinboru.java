/* RFA based broadcast publisher.
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
import com.reuters.rfa.common.Context;
import com.reuters.rfa.common.DeactivatedException;
import com.reuters.rfa.common.Dispatchable;
import com.reuters.rfa.common.DispatchException;
import com.reuters.rfa.common.EventQueue;
import com.reuters.rfa.common.Handle;
import com.reuters.rfa.common.PublisherPrincipalIdentity;
import com.reuters.rfa.config.ConfigDb;
import com.reuters.rfa.dictionary.DataDef;
import com.reuters.rfa.dictionary.DataDefDictionary;
import com.reuters.rfa.omm.OMMEncoder;
import com.reuters.rfa.omm.OMMFieldList;
import com.reuters.rfa.omm.OMMMap;
import com.reuters.rfa.omm.OMMMapEntry;
import com.reuters.rfa.omm.OMMMsg;
import com.reuters.rfa.omm.OMMState;
import com.reuters.rfa.omm.OMMTypes;
import com.reuters.rfa.rdm.RDMInstrument;
import com.reuters.rfa.rdm.RDMMsgTypes;
import com.reuters.rfa.session.Session;

public class Shinboru implements Provider.Delegate {

/* Application configuration. */
	private Config config;

/* RFA context. */
	private Rfa rfa;

/* RFA asynchronous event queue. */
	private EventQueue event_queue;

/* RFA provider */
	private Provider provider;

/* Identifier for this running application instance. */
	private PublisherPrincipalIdentity identity;

/* Instrument list. */
	private List<ItemStream> streams;

	private static Logger LOG = LogManager.getLogger (Shinboru.class.getName());
	private static Logger RFA_LOG = LogManager.getLogger ("com.reuters.rfa");

	private static final String RSSL_PROTOCOL		= "rssl";

/* RDM Usage Guide: Section 6.5: Enterprise Platform
 * For future compatibility, the DictionaryId should be set to 1 by providers.
 * The DictionaryId for the RDMFieldDictionary is 1.
 */
	private static final short  DICTIONARY_ID		= 1;
	private static final short  FIELD_LIST_ID		= 3;

	private static final int    OMM_PAYLOAD_SIZE            = 65535;

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

	private void init (CommandLine line, Options options) throws Exception {
		if (line.hasOption (HELP_OPTION)) {
			printHelp (options);
			return;
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

/* RFA Logging. */
// Remove existing handlers attached to j.u.l root logger
		SLF4JBridgeHandler.removeHandlersForRootLogger();
// add SLF4JBridgeHandler to j.u.l's root logger
		SLF4JBridgeHandler.install();

		if (RFA_LOG.isDebugEnabled()) {
			java.util.logging.Logger rfa_logger = java.util.logging.Logger.getLogger ("com.reuters.rfa");
			rfa_logger.setLevel (java.util.logging.Level.FINE);
		}

/* RFA Context. */
		this.rfa = new Rfa (this.config);
		this.rfa.init();

/* RFA asynchronous event queue. */
		this.event_queue = EventQueue.create (this.config.getEventQueueName());

/* RFA provider */
		this.provider = new Provider (this.config.getSession(),
					this.rfa,
					this.event_queue,
					this /* Provider.delegate */);
		this.provider.init();

/* Define this running instance identity. */
		this.identity = new PublisherPrincipalIdentity();
		this.identity.setPublisherAddress (UnsignedInts.toLong (InetAddresses.coerceToInteger (InetAddress.getLocalHost())));
		{
/* pre-Java 9: ProcessHandle.current().getPid() */
			RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
			String jvmName = runtimeBean.getName();
			long pid = Long.valueOf (jvmName.split("@")[0]);
			this.identity.setPublisherId (pid);
		}
		LOG.info ("Publisher identity: {}", this.identity);

/* Create state for published RIC. */
		this.streams = new ArrayList<ItemStream> (instruments.size());
		for (Instrument instrument : instruments) {
			ItemStream stream = new SymbolListStream (instrument.getName(), instrument.getSymbols());
			this.provider.createItemStream (instrument, stream);
			this.streams.add (stream);
			LOG.trace (instrument.toString());
		}

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
			if (null != this.app
				&& null != this.app.event_queue
				&& this.app.event_queue.isActive())
			{
				this.app.event_queue.deactivate();
				try {
					LOG.trace ("Waiting for mainloop shutdown ...");
					while (!this.app.is_shutdown) {
						Thread.sleep (100);
					}
					LOG.trace ("Shutdown complete.");
				} catch (InterruptedException e) {}
			}
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
		this.init (line, options);
		Thread shutdown_hook = new ShutdownThread (this);
		Runtime.getRuntime().addShutdownHook (shutdown_hook);
		LOG.trace ("Shutdown hook installed.");
		this.mainloop();
		LOG.trace ("Shutdown in progress.");
/* Cannot remove hook if shutdown is in progress. */
//		Runtime.getRuntime().removeShutdownHook (shutdown_hook);
//		LOG.trace ("Removed shutdown hook.");
		this.clear();
		this.is_shutdown = true;
	}

	public volatile boolean is_shutdown = false;

	private void drainqueue() {
		LOG.trace ("Draining event queue.");
		int count = 0;
		try {
			while (this.event_queue.dispatch (Dispatchable.NO_WAIT) > 0) { ++count; }
			LOG.trace ("Queue contained {} events.", count);
		} catch (DeactivatedException e) {
/* ignore on empty queue */
			if (count > 0) LOG.catching (e);
		} catch (Exception e) {
			LOG.catching (e);
		}
	}

	private void mainloop() {
		try {
			while (this.event_queue.isActive()) {
				this.event_queue.dispatch (Dispatchable.INFINITE_WAIT);
			}
		} catch (DeactivatedException e) {
/* manual shutdown */
			LOG.trace ("Mainloop deactivated.");
		} catch (Throwable t) {
			LOG.catching (t);
		} finally {
			if (!this.event_queue.isActive()) this.event_queue.deactivate();
			this.drainqueue();
		}
	}

/* Publish response for registered item stream. */
	@Override
	public void OnRequest (ItemStream stream, OMMEncoder encoder, OMMMsg msg, OMMState state) {
		SymbolListStream symbol_list_stream = (SymbolListStream)stream;

		msg.setMsgType (OMMMsg.MsgType.REFRESH_RESP);
		msg.setIndicationFlags (OMMMsg.Indication.REFRESH_COMPLETE);
		msg.setRespTypeNum (OMMMsg.RespType.UNSOLICITED);
		msg.setPrincipalIdentity (this.identity);

		state.setStreamState (OMMState.Stream.OPEN);
		state.setDataState (OMMState.Data.OK);
		state.setCode (OMMState.Code.NONE);
		msg.setState (state);

		encoder.initialize (OMMTypes.MSG, OMM_PAYLOAD_SIZE);
		encoder.encodeMsgInit (msg, OMMTypes.NO_DATA, OMMTypes.MAP);

		if (USE_DATA_DEFINITIONS)
		{
			encoder.encodeMapInit (OMMMap.HAS_DATA_DEFINITIONS | OMMMap.HAS_TOTAL_COUNT_HINT,
						OMMTypes.BUFFER /* key data type */, OMMTypes.FIELD_LIST /* value data type */,
						symbol_list_stream.getSymbols().size() /* total count hint */,
						(short)0 /* ignored */);
/* data definitions */
			encoder.encodeDataDefsInit();
			DataDefDictionary dictionary = DataDefDictionary.create (OMMTypes.FIELD_LIST_DEF_DB);
			DataDef dataDef = DataDef.create ((short)0 /* magic number */, OMMTypes.FIELD_LIST_DEF);
			dataDef.addDef ((short)3422, OMMTypes.RMTES_STRING);
			dictionary.putDataDef (dataDef);
			DataDefDictionary.encodeDataDef (dictionary, encoder, (short)0 /* magic number */);
			encoder.encodeDataDefsComplete();
			for (String symbol : symbol_list_stream.getSymbols()) {
				encoder.encodeMapEntryInit (0 /* flags */, OMMMapEntry.Action.ADD, null /* ignored */);
/* the key */
				encoder.encodeBytes (symbol.getBytes());
/* the value */
				encoder.encodeFieldListInit (OMMFieldList.HAS_DATA_DEF_ID | OMMFieldList.HAS_DEFINED_DATA,
								DICTIONARY_ID, FIELD_LIST_ID,
								(short)0 /* magic number */);
				encoder.encodeRmtesString (symbol);
			}
			encoder.encodeAggregateComplete();
		}
		else
		{
			encoder.encodeMapInit (OMMMap.HAS_TOTAL_COUNT_HINT,
						OMMTypes.BUFFER /* key data type */, OMMTypes.FIELD_LIST /* value data type */,
						symbol_list_stream.getSymbols().size() /* total count hint */,
						(short)0 /* ignored */);
			for (String symbol : symbol_list_stream.getSymbols()) {
				encoder.encodeMapEntryInit (0 /* flags */, OMMMapEntry.Action.ADD, null /* ignored */);
/* the key */
				encoder.encodeBytes (symbol.getBytes());
/* the value */
				encoder.encodeFieldListInit (OMMFieldList.HAS_STANDARD_DATA, DICTIONARY_ID, FIELD_LIST_ID, (short)0 /* ignored */);
				encoder.encodeFieldEntryInit ((short)3422, OMMTypes.RMTES_STRING);
				encoder.encodeRmtesString (symbol);
				encoder.encodeAggregateComplete();
			}
			encoder.encodeAggregateComplete();
		}

		this.provider.send (stream, (OMMMsg)encoder.getEncodedObject());
	}

	private void clear() {
/* Prevent new events being generated whilst shutting down. */
		if (null != this.event_queue && this.event_queue.isActive()) {
			LOG.trace ("Deactivating EventQueue.");
			this.event_queue.deactivate();
			this.drainqueue();
		}

		if (null != this.provider) {
			LOG.trace ("Closing Provider.");
			this.provider.clear();
			this.provider = null;
		}

		if (null != this.event_queue) {
			LOG.trace ("Closing EventQueue.");
			this.event_queue.destroy();
			this.event_queue = null;
		}

		if (null != this.rfa) {
			LOG.trace ("Closing RFA.");
			this.rfa.clear();
			this.rfa = null;
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
