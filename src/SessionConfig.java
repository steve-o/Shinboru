/* Adapter per session configuration.
 */

import com.google.common.base.Optional;
import com.google.gson.Gson;

public class SessionConfig {
//  Protocol name, RSSL or SSL.
	private String protocol;

//  TREP-RT ADH hostname or IP address.
	private String[] servers;

//  TREP-RT service name, e.g. IDN_RDF.
	private String service_name;

//  Default TREP-RT R/SSL port, e.g. 14002, 14003, 8101.
	private Optional<String> default_port;

/* DACS application Id.  If the server authenticates with DACS, the provider
 * application may be required to pass in a valid ApplicationId.
 * Range: "" (None) or 1-511 as an Ascii string.
 */
	private Optional<String> application_id;

/* InstanceId is used to differentiate applications running on the same host.
 * If there is more than one noninteractive provider instance running on the
 * same host, they must be set as a different value by the provider
 * application. Otherwise, the infrastructure component which the providers
 * connect to will reject a login request that has the same InstanceId value
 * and cut the connection.
 * Range: "" (None) or any Ascii string, presumably to maximum RFA_String length.
 */
	private Optional<String> instance_id;

/* DACS username, frequently non-checked and set to similar: user1.
 */
	private Optional<String> user_name;

/* DACS position, the station which the user is using.
 * Range: "" (None) or "<IPv4 address>/hostname" or "<IPv4 address>/net"
 */
	private Optional<String> position;

/* Local dictionary files to override source delivered version.
 */
	private Optional<String> field_dictionary;
	private Optional<String> enum_dictionary;

	public SessionConfig (String protocol, String[] servers, String service_name) {
		this.protocol = protocol;
		this.servers = servers;
		this.service_name = service_name;
		this.default_port = Optional.absent();
		this.application_id = Optional.absent();
		this.instance_id = Optional.absent();
		this.user_name = Optional.absent();
		this.position = Optional.absent();
		this.field_dictionary = Optional.absent();
		this.enum_dictionary = Optional.absent();
	}

	public String getProtocol() {
		return this.protocol;
	}

	public String[] getServers() {
		return this.servers;
	}

	public String getServiceName() {
		return this.service_name;
	}

/* optional parameters */
	public boolean hasDefaultPort() {
		return this.default_port.isPresent();
	}

	public String getDefaultPort() {
		return this.default_port.get();
	}

	public void setDefaultPort (String default_port) {
		this.default_port = Optional.of (default_port);
	}

	public boolean hasApplicationId() {
		return this.application_id.isPresent();
	}

	public String getApplicationId() {
		return this.application_id.get();
	}

	public void setApplicationId (String application_id) {
		this.application_id = Optional.of (application_id);
	}

	public boolean hasInstanceId() {
		return this.instance_id.isPresent();
	}

	public String getInstanceId() {
		return this.instance_id.get();
	}

	public void setInstanceId (String instance_id) {
		this.instance_id = Optional.of (instance_id);
	}

	public boolean hasUserName() {
		return this.user_name.isPresent();
	}

	public String getUserName() {
		return this.user_name.get();
	}

	public void setUserName (String user_name) {
		this.user_name = Optional.of (user_name);
	}

	public boolean hasPosition() {
		return this.position.isPresent();
	}

	public String getPosition() {
		return this.position.get();
	}

	public void setPosition (String position) {
		this.position = Optional.of (position);
	}

	public boolean hasFieldDictionary() {
		return this.field_dictionary.isPresent();
	}
        
	public String getFieldDictionary() {
		return this.field_dictionary.get();
	}
        
	public void setFieldDictionary (String dictionary) {
		this.field_dictionary = Optional.of (dictionary);
	}
        
	public boolean hasEnumDictionary() {
		return this.enum_dictionary.isPresent();
	}

	public String getEnumDictionary() {
		return this.enum_dictionary.get();
	}

	public void setEnumDictionary (String dictionary) {
		this.enum_dictionary = Optional.of (dictionary);
	}

	@Override
	public String toString() {
		return new Gson().toJson (this);
	}
}

/* eof */
