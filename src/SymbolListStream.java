/* Item stream runtime specialized for symbol lists.
 */

import java.util.Collection;
import com.google.common.collect.ImmutableSet;
import com.reuters.rfa.rdm.RDMMsgTypes;

public class SymbolListStream extends ItemStream {
	private ImmutableSet<String> set;

	public SymbolListStream (String item_name, Collection<String> symbols) {
		super (item_name, RDMMsgTypes.SYMBOL_LIST);
		this.set = ImmutableSet.copyOf (symbols);
	}

	public ImmutableSet<String> getSymbols() {
		return this.set;
	}
}

/* eof */
