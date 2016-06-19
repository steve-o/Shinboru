/* Simple provider.
 */

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.*;
import java.net.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.reuters.rfa.common.Client;
import com.reuters.rfa.common.Context;
import com.reuters.rfa.common.Event;
import com.reuters.rfa.common.EventQueue;
import com.reuters.rfa.common.EventSource;
import com.reuters.rfa.common.Handle;
import com.reuters.rfa.dictionary.FieldDictionary;
import com.reuters.rfa.omm.OMMArray;
import com.reuters.rfa.omm.OMMData;
import com.reuters.rfa.omm.OMMDataBuffer;
import com.reuters.rfa.omm.OMMElementEntry;
import com.reuters.rfa.omm.OMMElementList;
import com.reuters.rfa.omm.OMMEncoder;
import com.reuters.rfa.omm.OMMEntry;
import com.reuters.rfa.omm.OMMFilterEntry;
import com.reuters.rfa.omm.OMMFilterList;
import com.reuters.rfa.omm.OMMMap;
import com.reuters.rfa.omm.OMMMapEntry;
import com.reuters.rfa.omm.OMMMsg;
import com.reuters.rfa.omm.OMMPool;
import com.reuters.rfa.omm.OMMState;
import com.reuters.rfa.omm.OMMTypes;
import com.reuters.rfa.rdm.RDMInstrument;
import com.reuters.rfa.rdm.RDMMsgTypes;
import com.reuters.rfa.rdm.RDMService;
import com.reuters.rfa.session.Session;
import com.reuters.rfa.session.TimerIntSpec;
import com.reuters.rfa.session.omm.OMMConnectionEvent;
import com.reuters.rfa.session.omm.OMMConnectionIntSpec;
import com.reuters.rfa.session.omm.OMMConnectionStatsEvent;
import com.reuters.rfa.session.omm.OMMConnectionStatsIntSpec;
import com.reuters.rfa.session.omm.OMMErrorIntSpec;
import com.reuters.rfa.session.omm.OMMItemCmd;
import com.reuters.rfa.session.omm.OMMItemEvent;
import com.reuters.rfa.session.omm.OMMItemIntSpec;
import com.reuters.rfa.session.omm.OMMProvider;
import com.thomsonreuters.rfa.valueadd.domainrep.ResponseStatus;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.dictionary.RDMDictionary;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.dictionary.RDMDictionaryCache;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.dictionary.RDMDictionaryRequest;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.dictionary.RDMDictionaryRequestAttrib;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.dictionary.RDMDictionaryResponse;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.directory.RDMDirectory;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.directory.RDMDirectoryRequest;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.directory.RDMDirectoryRequestAttrib;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.directory.RDMDirectoryResponse;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.directory.RDMDirectoryResponseAttrib;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.directory.RDMDirectoryResponsePayload;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.directory.Service;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.login.RDMLogin;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.login.RDMLoginRequest;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.login.RDMLoginRequestAttrib;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.login.RDMLoginResponse;
import com.thomsonreuters.rfa.valueadd.domainrep.rdm.login.RDMLoginResponseAttrib;

public class Provider implements Client {
	private static Logger LOG = LogManager.getLogger (Provider.class.getName());
	private static final String LINE_SEPARATOR = System.getProperty ("line.separator");

	private SessionConfig config;

/* RFA context. */
	private Rfa rfa;

/* RFA asynchronous event queue. */
	private EventQueue event_queue;

/* RFA session defines one or more connections for horizontal scaling. */
	private Session session;

/* RFA OMM provider interface. */
	private OMMProvider omm_provider;
        private OMMPool omm_pool;
	private OMMEncoder omm_encoder;

	private StringBuilder sb;

/* Data dictionaries. */
	private RDMDictionaryCache rdm_dictionary;

	public interface Delegate {
		public void OnRequest (ItemStream stream, OMMEncoder encoder, OMMMsg msg, OMMState state);
	}

	private Delegate delegate;

/* Directory */
	private Map<String, ItemStream> directory;

/* RFA Item event provider */
	private Handle connection_handle;
	private Handle stats_handle;
	private Handle error_handle;
	private Handle login_handle;
/* no directory handle for a provider */

	private class FlaggedHandle {
		private Handle handle;
		private boolean flag;

		public FlaggedHandle (Handle handle) {
			this.handle = handle;
			this.flag = false;
		}

		public Handle getHandle() {
			return this.handle;
		}

		public boolean isFlagged() {
			return this.flag;
		}

		public void setFlag() {
			this.flag = true;
		}
	}

	private Map<String, FlaggedHandle> dictionary_handle;
	private ImmutableMap<String, Integer> appendix_a;

/* Reuters Wire Format versions. */
	private byte rwf_major_version;
	private byte rwf_minor_version;

	private boolean is_muted;
	private boolean pending_dictionary;

	private static final boolean CLOSE_ON_SHUTDOWN = false;
	private static final boolean UNSUBSCRIBE_ON_SHUTDOWN = false;

	private static final int OMM_PAYLOAD_SIZE       = 5000;

	private static final String RSSL_PROTOCOL       = "rssl";

/* Hard coded for Elektron and TREP. */
	private static final String FIELD_DICTIONARY_NAME = "RWFFld";
	private static final String ENUM_TYPE_DICTIONARY_NAME = "RWFEnum";

	public Provider (SessionConfig config, Rfa rfa, EventQueue event_queue, Delegate delegate) {
		this.config = config;
		this.rfa = rfa;
		this.event_queue = event_queue;
		this.delegate = delegate;
		this.rwf_major_version = 0;
		this.rwf_minor_version = 0;
		this.is_muted = true;
		this.pending_dictionary = false;

		if (this.config.getProtocol().equalsIgnoreCase (RSSL_PROTOCOL))
		{
			this.pending_dictionary = true;
		}
	}

	public void init() throws Exception {
		LOG.trace (this.config);

/* Manual serialisation */
		this.sb = new StringBuilder (512);

/* Configuring the session layer package.
 */
		LOG.trace ("Acquiring RFA session.");
		this.session = Session.acquire (this.config.getSessionName());

/* RFA Version Info. The version is only available if an application
 * has acquired a Session (i.e., the Session Layer library is laoded).
 */
		LOG.info ("RFA: { \"productVersion\": \"{}\" }", Context.getRFAVersionInfo().getProductVersion());

		if (this.config.getProtocol().equalsIgnoreCase (RSSL_PROTOCOL))
		{
/* Initializing an OMM provider. */
			LOG.trace ("Creating OMM provider.");
			this.omm_provider = (OMMProvider)this.session.createEventSource (EventSource.OMM_PROVIDER,
						this.config.getProviderName(),
						false /* complete events */);

/* OMM memory management. */
			this.omm_pool = OMMPool.create (OMMPool.SINGLE_THREADED);
			this.omm_encoder = this.omm_pool.acquireEncoder();
			this.omm_encoder.initialize (OMMTypes.MSG, OMM_PAYLOAD_SIZE);

			this.rdm_dictionary = new RDMDictionaryCache();

			this.sendLoginRequest();

/* Registering for Events from an OMM Provider after instead of before the login request. */
			LOG.trace ("Registering OMM error interest.");
			OMMErrorIntSpec ommErrorIntSpec = new OMMErrorIntSpec();
			error_handle = this.omm_provider.registerClient (this.event_queue, ommErrorIntSpec, this, null /* closure */);

			LOG.trace ("Registering OMM connection interest.");
			OMMConnectionIntSpec ommConnIntSpec = new OMMConnectionIntSpec();
			connection_handle = this.omm_provider.registerClient (this.event_queue, ommConnIntSpec, this, null /* closure */);

			LOG.trace ("Registering OMM stats interest.");
			OMMConnectionStatsIntSpec ommConnStatsIntSpec = new OMMConnectionStatsIntSpec();
			ommConnStatsIntSpec.setStatsInterval (60 * 1000 /* milliseconds */);
			stats_handle = this.omm_provider.registerClient (this.event_queue, ommConnStatsIntSpec, this, null /* closure */);
		}
		else
		{
			throw new Exception ("Unsupported transport protocol \"" + this.config.getProtocol() + "\".");
		}

		this.directory = new LinkedHashMap<String, ItemStream>();
		this.dictionary_handle = new TreeMap<String, FlaggedHandle>();
	}

	public void clear() {
		if (null != this.rdm_dictionary)
			this.rdm_dictionary = null;
		if (null != this.omm_encoder)
			this.omm_encoder = null;
		if (null != this.omm_pool) {
			LOG.trace ("Closing OMMPool.");
			this.omm_pool.destroy();
			this.omm_pool = null;
		}
		if (null != this.omm_provider) {
			LOG.trace ("Closing OMMProvider.");
/* 8.2.11 Shutting Down an Application
 * an application may just destroy Event
 * Source, in which case the closing of the streams is handled by the RFA.
 */
			if (CLOSE_ON_SHUTDOWN) {
				if (null != this.directory && !this.directory.isEmpty()) {
					List<Handle> item_handles = new ArrayList<Handle> (this.directory.size());
					for (ItemStream item_stream : this.directory.values()) {
						this.sendClose (item_stream);
					}
				}
			}
			this.dictionary_handle.clear();
			if (UNSUBSCRIBE_ON_SHUTDOWN) {
				if (null != this.stats_handle) {
					this.omm_provider.unregisterClient (this.stats_handle);
					this.stats_handle = null;
				}
				if (null != this.connection_handle) {
					this.omm_provider.unregisterClient (this.connection_handle);
					this.connection_handle = null;
				}
				if (null != this.error_handle) {
					this.omm_provider.unregisterClient (this.error_handle);
					this.error_handle = null;
				}
				if (null != this.login_handle) {
					this.omm_provider.unregisterClient (this.login_handle);
					this.login_handle = null;
				}
			} else {
				if (null != this.directory && !this.directory.isEmpty())
					this.directory.clear();
				if (null != this.dictionary_handle && !this.dictionary_handle.isEmpty())
					this.dictionary_handle.clear();
				if (null != this.stats_handle)
					this.stats_handle = null;
				if (null != this.connection_handle)
					this.connection_handle = null;
				if (null != this.error_handle)
					this.error_handle = null;
				if (null != this.login_handle)
					this.login_handle = null;
			}
			this.omm_provider.destroy();
			this.omm_provider = null;
		}
		if (null != this.session) {
			LOG.trace ("Closing RFA Session.");
			this.session.release();
			this.session = null;
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
			if (this.config.getProtocol().equalsIgnoreCase (RSSL_PROTOCOL)) {
				item_stream.setToken (this.omm_provider.generateToken());
			}
		}
		this.directory.put (key, item_stream);
		LOG.trace ("Directory size: {}", this.directory.size());
	}

	public int send (ItemStream stream, OMMMsg msg) {
		OMMItemCmd cmd = new OMMItemCmd();
		cmd.setMsg (msg);
		cmd.setToken (stream.getToken());
		if (LOG.isDebugEnabled()) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintStream ps = new PrintStream (baos);
			GenericOMMParser.parseMsg (cmd.getMsg(), ps);
			LOG.debug ("Sending:{}{}", LINE_SEPARATOR, baos.toString());
		}
		final int command_id = this.omm_provider.submit (cmd, stream);
		return command_id;
	}

	public void sendClose (ItemStream item_stream) {
		OMMMsg msg = this.omm_pool.acquireMsg();
		msg.setAssociatedMetaInfo (this.login_handle);
		msg.setAttribInfo (this.config.getServiceName(), item_stream.getItemName(), RDMInstrument.NameType.RIC);
		msg.setMsgType (OMMMsg.MsgType.STATUS_RESP);
		msg.setMsgModelType (item_stream.getMsgModelType());

		OMMState state = this.omm_pool.acquireState();
		state.setStreamState (OMMState.Stream.CLOSED);
		state.setDataState (OMMState.Data.NO_CHANGE);
		state.setCode (OMMState.Code.NONE);
		msg.setState (state);

		this.omm_encoder.initialize (OMMTypes.MSG, OMM_PAYLOAD_SIZE);
		this.omm_encoder.encodeMsg (msg);
		this.send (item_stream, (OMMMsg)this.omm_encoder.getEncodedObject());

		this.omm_pool.releaseMsg (msg);
	}

/* Making a Login Request
 * A Login request message is encoded and sent by OMM Consumer and OMM non-
 * interactive provider applications.
 */
	private void sendLoginRequest() throws UnknownHostException {
		LOG.info ("Sending login request.");
		RDMLoginRequest request = new RDMLoginRequest();
		RDMLoginRequestAttrib attribInfo = new RDMLoginRequestAttrib();

/* RFA/Java only.
 */
		request.setMessageType (RDMLoginRequest.MessageType.REQUEST);
		request.setIndicationMask (EnumSet.of (RDMLoginRequest.IndicationMask.REFRESH));
		attribInfo.setRole (RDMLogin.Role.PROVIDER_NON_INTERACTIVE);

/* DACS username (required).
 */
		attribInfo.setNameType (RDMLogin.NameType.USER_NAME);
		attribInfo.setName (this.config.hasUserName() ?
			this.config.getUserName()
			: System.getProperty ("user.name"));

/* DACS Application Id (optional).
 * e.g. "256"
 */
		if (this.config.hasApplicationId())
			attribInfo.setApplicationId (this.config.getApplicationId());

/* DACS Position name (optional).
 * e.g. "127.0.0.1/net"
 */
		if (this.config.hasPosition()) {
			if (!this.config.getPosition().isEmpty())
				attribInfo.setPosition (this.config.getPosition());
		} else {
			this.sb.setLength (0);
			this.sb .append (InetAddress.getLocalHost().getHostAddress())
				.append ('/')
				.append (InetAddress.getLocalHost().getHostName());
			attribInfo.setPosition (this.sb.toString());
		}

/* Instance Id (optional).
 * e.g. "<Instance Id>"
 */
		if (this.config.hasInstanceId())
			attribInfo.setInstanceId (this.config.getInstanceId());

		request.setAttrib (attribInfo);

		LOG.trace ("Registering OMM item interest for MMT_LOGIN.");
		OMMMsg msg = request.getMsg (this.omm_pool);
		if (LOG.isDebugEnabled()) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintStream ps = new PrintStream (baos);
			GenericOMMParser.parseMsg (msg, ps);
			LOG.debug ("Sending:{}{}", LINE_SEPARATOR, baos.toString());
		}
		OMMItemIntSpec ommItemIntSpec = new OMMItemIntSpec();
		ommItemIntSpec.setMsg (msg);
		this.login_handle = this.omm_provider.registerClient (this.event_queue, ommItemIntSpec, this, null);
	}

	private void sendDirectoryResponse() {
		LOG.trace ("Sending directory response.");
		RDMDirectoryResponse response = new RDMDirectoryResponse();
		RDMDirectoryResponseAttrib attribInfo = new RDMDirectoryResponseAttrib();
		RDMDirectoryResponsePayload payload = new RDMDirectoryResponsePayload();
		RDMDirectory.ServiceList service_list = new RDMDirectory.ServiceList();

		response.setMessageType (RDMDirectoryResponse.MessageType.REFRESH_RESP);
		response.setIsRefreshSolicited (false);
		response.setIndicationMask (EnumSet.of (RDMDirectoryResponse.IndicationMask.REFRESH_COMPLETE));

		ResponseStatus status = new ResponseStatus();
		status.setStreamState (OMMState.Stream.OPEN);
		status.setDataState (OMMState.Data.OK);
		status.setCode (OMMState.Code.NONE);
		response.setResponseStatus (status);

		attribInfo.setFilterMask (EnumSet.of (RDMDirectory.FilterMask.INFO, RDMDirectory.FilterMask.STATE));
		response.setAttrib (attribInfo);

		Service service = new Service();
		service.setServiceName (this.config.getServiceName());
		service.setInfoFilter (this.getServiceInformation());
		service.setStateFilter (this.getServiceState());
		service.setAction (RDMDirectory.ServiceAction.ADD);
		service_list.add (service);
		payload.setServiceList (service_list);
		response.setPayload (payload);

		OMMMsg msg = response.getMsg (this.omm_pool);
		if (LOG.isDebugEnabled()) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintStream ps = new PrintStream (baos);
			GenericOMMParser.parseMsg (msg, ps);
			LOG.debug ("Sending:{}{}", LINE_SEPARATOR, baos.toString());
		}
		OMMItemCmd cmd = new OMMItemCmd();
		cmd.setMsg (msg);
		cmd.setToken (this.omm_provider.generateToken());
		this.omm_provider.submit (cmd, null /* closure */);
	}

	private Service.InfoFilter getServiceInformation() {
		Service.InfoFilter info = new Service.InfoFilter();

		info.setServiceName (this.config.getServiceName());
		info.setCapabilityList (this.getServiceCapabilities());
		info.setDictionaryUsedList (this.getServiceDictionaries());

/* only src_dist requires a QoS */

		return info;
	}

	private RDMDirectory.CapabilityList getServiceCapabilities() {
		RDMDirectory.CapabilityList capabilities = new RDMDirectory.CapabilityList();

		capabilities.add (RDMDirectory.Capability.SYMBOL_LIST);

		return capabilities;
	}

	private RDMDirectory.DictionaryUsedList getServiceDictionaries() {
		RDMDirectory.DictionaryUsedList dictionaries = new RDMDirectory.DictionaryUsedList();

		dictionaries.add (FIELD_DICTIONARY_NAME);
		dictionaries.add (ENUM_TYPE_DICTIONARY_NAME);

		return dictionaries;
	}

	private Service.StateFilter getServiceState() {
		Service.StateFilter state = new Service.StateFilter();

		state.setServiceUp (true);

		return state;
	}

/* Make a dictionary request.
 *
 * 5.8.3 Version Check
 * Dictionary version checking can be performed by the client after a refresh
 * (Section 2.2) response message of a Dictionary is received.
 */
	private void sendDictionaryRequest (String dictionary_name) {
		LOG.info ("Sending dictionary request for \"{}\".", dictionary_name);
		RDMDictionaryRequest request = new RDMDictionaryRequest();
		RDMDictionaryRequestAttrib attribInfo = new RDMDictionaryRequestAttrib();

/* RFA/Java only.
 */
		request.setMessageType (RDMDictionaryRequest.MessageType.REQUEST);
		request.setIndicationMask (EnumSet.of (RDMDictionaryRequest.IndicationMask.REFRESH, RDMDictionaryRequest.IndicationMask.NONSTREAMING));

// RDMDictionary.Filter.NORMAL=0x7: Provides all information needed for decoding
		attribInfo.setVerbosity (RDMDictionary.Verbosity.NORMAL);
		attribInfo.setServiceName (this.config.getServiceName());
		attribInfo.setDictionaryName (dictionary_name);

		request.setAttrib (attribInfo);

		LOG.debug ("Registering OMM item interest for MMT_DICTIONARY/{}", dictionary_name);
		OMMMsg msg = request.getMsg (this.omm_pool);
		if (LOG.isDebugEnabled()) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			PrintStream ps = new PrintStream (baos);
			GenericOMMParser.parseMsg (msg, ps);
			LOG.debug ("Sending:{}{}", LINE_SEPARATOR, baos.toString());
		}
		OMMItemIntSpec ommItemIntSpec = new OMMItemIntSpec();
		ommItemIntSpec.setMsg (msg);
		this.dictionary_handle.put (dictionary_name,
			new FlaggedHandle (this.omm_provider.registerClient (this.event_queue, ommItemIntSpec, this, dictionary_name /* closure */)));
	}

	@Override
	public void processEvent (Event event) {
		LOG.trace (event);
		switch (event.getType()) {
		case Event.OMM_ITEM_EVENT:
			this.OnOMMItemEvent ((OMMItemEvent)event);
			break;

/* RFA 7.5.1 */
		case Event.OMM_CONNECTION_EVENT:
			this.OnConnectionEvent ((OMMConnectionEvent)event);
			break;

/* RFA 8.0.0 */
		case Event.OMM_CONNECTION_STATS_EVENT:
			this.OnConnectionStatsEvent ((OMMConnectionStatsEvent)event);
			break;

		default:
			LOG.trace ("Uncaught: {}", event);
			break;
		}
	}

/* Handling Item Events, message types are munged c.f. C++ API.
 */
	private void OnOMMItemEvent (OMMItemEvent event) {
		LOG.trace ("OnOMMItemEvent: {}", event);
		final OMMMsg msg = event.getMsg();

/* Verify event is a response event. */
		switch (msg.getMsgType()) {
		case OMMMsg.MsgType.REFRESH_RESP:
		case OMMMsg.MsgType.UPDATE_RESP:
		case OMMMsg.MsgType.STATUS_RESP:
		case OMMMsg.MsgType.ACK_RESP:
			this.OnRespMsg (msg, event.getHandle(), event.getClosure());
			break;

/* Request message */
		case OMMMsg.MsgType.REQUEST:
/* Generic message */
		case OMMMsg.MsgType.GENERIC:
/* Post message */
		case OMMMsg.MsgType.POST:
		default:
			LOG.trace ("Uncaught: {}", msg);
			break;
		}
	}

	private void OnRespMsg (OMMMsg msg, Handle handle, Object closure) {
		LOG.trace ("OnRespMsg: {}", msg);
		switch (msg.getMsgModelType()) {
		case RDMMsgTypes.LOGIN:
			this.OnLoginResponse (msg);
			break;

		case RDMMsgTypes.DICTIONARY:
			this.OnDictionaryResponse (msg, handle, closure);
			break;

		default:
			LOG.trace ("Uncaught: {}", msg);
			break;
		}
	}

	private void OnLoginResponse (OMMMsg msg) {
		LOG.trace ("OnLoginResponse: {}", msg);
/* RFA example helper to dump incoming message. */
//GenericOMMParser.parse (msg);
		final RDMLoginResponse response = new RDMLoginResponse (msg);
		final byte stream_state = response.getRespStatus().getStreamState();
		final byte data_state   = response.getRespStatus().getDataState();

		switch (stream_state) {
		case OMMState.Stream.OPEN:
			switch (data_state) {
			case OMMState.Data.OK:
				this.OnLoginSuccess (response);
				break;

			case OMMState.Data.SUSPECT:
				this.OnLoginSuspect (response);
				break;

			default:
				LOG.trace ("Uncaught data state: {}", response);
				break;
			}
			break;

		case OMMState.Stream.CLOSED:
			this.OnLoginClosed (response);
			break;

		default:
			LOG.trace ("Uncaught stream state: {}", response);
			break;
		}
	}

/* Login Success.
 */
	private void OnLoginSuccess (RDMLoginResponse response) {
		LOG.trace ("OnLoginSuccess: {}", response);
/* Ignore duplicate status responses, rather odd API. */
		if (!response.hasIsRefreshSolicited() || !response.getIsRefreshSolicited()) {
			return;
		}

		LOG.info ("Logged into infrastructure.");

		if (this.pending_dictionary) {
/* Detect support for provider dictionary download, not implemented in RFA/Java 8.0.1 */
			this.sendDictionaryRequest (FIELD_DICTIONARY_NAME);
			this.sendDictionaryRequest (ENUM_TYPE_DICTIONARY_NAME);
		} else {
			this.sendDirectoryResponse();
			this.resetTokens();
			LOG.info ("Unmuting provider.");
			this.is_muted = false;
			this.republish();
		}
	}

	private void resetTokens() {
		LOG.debug ("Resetting {} provider tokens.", this.directory.size());
		for (ItemStream item_stream : this.directory.values()) {
			item_stream.setToken (this.omm_provider.generateToken());
		}
	}

/* Other Login States.
 */
	private void OnLoginSuspect (RDMLoginResponse response) {
		LOG.warn ("OnLoginSuspect: {}", response);
		this.is_muted = true;
	}

/* Other Login States.
 */
	private void OnLoginClosed (RDMLoginResponse response) {
		LOG.warn ("OnLoginClosed: {}", response);
		this.is_muted = true;
	}

/* MMT_DICTIONARY domain.
 *
 * 5.8.4 Streaming Dictionary
 * Dictionary request can be streaming. Dictionary providers are not allowed to
 * send refresh and update data to consumers.  Instead the provider can
 * advertise a minor Dictionary change by sending a status (Section 2.2)
 * response message with a DataState of Suspect. It is the consumer’s
 * responsibility to reissue the dictionary request.
 */
	private void OnDictionaryResponse (OMMMsg msg, Handle handle, Object closure) {
		LOG.trace ("OnDictionaryResponse: {}", msg);
		final RDMDictionaryResponse response = new RDMDictionaryResponse (msg);
/* Receiving dictionary */
		if (response.hasAttrib()) {
			LOG.debug ("Dictionary {}: {}", response.getMessageType(), response.getAttrib().getDictionaryName());
		}
		if (response.getMessageType() == RDMDictionaryResponse.MessageType.REFRESH_RESP
			&& response.hasPayload() && null != response.getPayload())
		{
			this.rdm_dictionary.load (response.getPayload(), handle);
		}

/* Only know type after it is loaded. */
		final RDMDictionary.DictionaryType dictionary_type = this.rdm_dictionary.getDictionaryType (handle);

/* Received complete dictionary */
		if (response.getMessageType() == RDMDictionaryResponse.MessageType.REFRESH_RESP
			&& response.getIndicationMask().contains (RDMDictionaryResponse.IndicationMask.REFRESH_COMPLETE))
		{
			LOG.debug ("Dictionary refresh is marked complete.");
/* Check dictionary version */
			FieldDictionary field_dictionary = this.rdm_dictionary.getFieldDictionary();
			if (RDMDictionary.DictionaryType.RWFFLD == dictionary_type)
			{
				LOG.info ("Loaded dictionary: RDM field definitions version: {}", field_dictionary.getFieldProperty ("Version"));
			}
			else if (RDMDictionary.DictionaryType.RWFENUM == dictionary_type)
			{
/* Interesting values like Name, RT_Version, Description, Date are not provided by ADS */
				LOG.info ("Loaded dictionary: RDM enumerated tables version: {}", field_dictionary.getEnumProperty ("DT_Version"));
			}
/* Notify RFA example helper of dictionary if using to dump message content. */
GenericOMMParser.initializeDictionary (field_dictionary);
			this.dictionary_handle.get ((String)closure).setFlag();

/* Check all pending dictionaries */
			int pending_dictionaries = this.dictionary_handle.size();
			for (FlaggedHandle flagged_handle : this.dictionary_handle.values()) {
				if (flagged_handle.isFlagged())
					--pending_dictionaries;
			}
			if (0 == pending_dictionaries) {
				LOG.info ("All used dictionaries loaded, sending directory.");
				this.pending_dictionary = false;
				this.sendDirectoryResponse();
				this.resetTokens();
				LOG.info ("Unmuting provider.");
				this.is_muted = false;
				this.republish();
			} else {
				LOG.info ("Dictionaries pending: {}", pending_dictionaries);
			}
		}
	}

	private void republish() {
		for (ItemStream item_stream : this.directory.values()) {
			OMMMsg msg = this.omm_pool.acquireMsg();
			OMMState state = this.omm_pool.acquireState();
			msg.setMsgModelType (item_stream.getMsgModelType());
			msg.setAssociatedMetaInfo (this.login_handle);
			msg.setAttribInfo (this.config.getServiceName(), item_stream.getItemName(), RDMInstrument.NameType.RIC);
			this.delegate.OnRequest (item_stream, this.omm_encoder, msg, state);
			this.omm_pool.releaseState (state);
			this.omm_pool.releaseMsg (msg);
		}
	}

	private void OnConnectionEvent (OMMConnectionEvent event) {
		LOG.info ("OnConnectionEvent: {}", event);
	}

	private void OnConnectionStatsEvent (OMMConnectionStatsEvent event) {
		LOG.info ("OnConnectionStatsEvent: {}", event);
	}
}

/* eof */
