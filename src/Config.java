/* Adapter configuration.
 */

import com.google.gson.Gson;

public class Config {
//  RFA sessions comprising of session names, connection names,
//  RSSL hostname or IP address and default RSSL port, e.g. 14002, 14003.
	private SessionConfig[] sessions;

//  File containing list of symbols
	private String symbol_path;

	public SessionConfig[] getSessions() {
		return this.sessions;
	}

	public SessionConfig getSession() {
		return this.getSessions()[0];
	}

	public void setSessions (SessionConfig[] sessions) {
		this.sessions = sessions;
	}

	public String getSymbolPath() {
		return this.symbol_path;
	}

	public void setSymbolPath (String symbol_path) {
		this.symbol_path = symbol_path;
	}

	@Override
	public String toString() {
		return new Gson().toJson (this);
	}
}

/* eof */
