/* Item stream runtime specialized for symbol lists.
 */

import java.util.Collection;
import com.google.common.collect.ImmutableSet;
import com.thomsonreuters.upa.rdm.DomainTypes;

public class SymbolListStream extends ItemStream {
	private ImmutableSet<String> set;

	public SymbolListStream (String item_name, Collection<String> symbols) {
		super (item_name, DomainTypes.SYMBOL_LIST);
		this.set = ImmutableSet.copyOf (symbols);
	}

	public ImmutableSet<String> getSymbols() {
		return this.set;
	}
}

/* eof */
