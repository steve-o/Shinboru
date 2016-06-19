/* Publish symbol.
 */

import java.util.Collection;
import com.google.gson.Gson;
import com.google.common.collect.ImmutableSet;

public class Instrument {
	private String name;
	private ImmutableSet<String> set;

	public Instrument (String name, Collection<String> symbols) {
		this.setName (name);
		this.set = ImmutableSet.copyOf (symbols);
	}

	public String getName() {
		return this.name;
	}

	public void setName (String name) {
		this.name = name;
	}

	public ImmutableSet<String> getSymbols() {
		return this.set;
	}

	@Override
	public String toString() {
		return new Gson().toJson (this);
	}
}

/* eof */
