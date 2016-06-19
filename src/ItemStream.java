/* Item stream runtime.
 */

import com.google.common.base.Optional;
import com.reuters.rfa.common.Token;

public class ItemStream {
/* Fixed name for this stream. */
	private String item_name;

/* RDM domain */
	private short mmt;

/* Session token. */
	private Optional<Token> token;

	public ItemStream (String item_name, short mmt) {
		this.item_name = item_name;
		this.mmt = mmt;
		this.clearToken();
	}

	public String getItemName() {
		return this.item_name;
	}

	public short getMsgModelType() {
		return this.mmt;
	}

	public Token getToken() {
		return this.token.get();
	}

	public boolean hasToken() {
		return this.token.isPresent();
	}

	public void setToken (Token token) {
		this.token = Optional.of (token);
	}

	public void clearToken() {
		this.token = Optional.absent();
	}

}

/* eof */
