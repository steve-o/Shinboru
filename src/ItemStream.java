/* Item stream runtime.
 */

import com.google.common.base.Optional;

public class ItemStream {
/* Fixed name for this stream. */
	private String item_name;

/* RDM domain */
	private int mmt;

/* Session token. */
	private Optional<Integer> token;

	public ItemStream (String item_name, int mmt) {
		this.item_name = item_name;
		this.mmt = mmt;
		this.clearToken();
	}

	public String getItemName() {
		return this.item_name;
	}

	public int getMsgModelType() {
		return this.mmt;
	}

	public int getToken() {
		return this.token.get();
	}

	public boolean hasToken() {
		return this.token.isPresent();
	}

	public void setToken (int token) {
		this.token = Optional.of (token);
	}

	public void clearToken() {
		this.token = Optional.absent();
	}

}

/* eof */
