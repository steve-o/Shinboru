/* Upa context.
 */

import java.net.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.common.base.Joiner;
import com.thomsonreuters.upa.transport.InitArgs;
import com.thomsonreuters.upa.transport.LibraryVersionInfo;
import com.thomsonreuters.upa.transport.Transport;
import com.thomsonreuters.upa.transport.TransportFactory;
import com.thomsonreuters.upa.transport.TransportReturnCodes;

public class Upa {

	private static Logger LOG = LogManager.getLogger (Upa.class.getName());

	private Config config;

	public Upa (Config config) {
		this.config = config;
	}

	public boolean Initialize() {
		LOG.trace ("Initializing UPA.");
		final com.thomsonreuters.upa.transport.Error rssl_err = TransportFactory.createError();
		final InitArgs initArgs = TransportFactory.createInitArgs();
		initArgs.globalLocking (false);
		if (TransportReturnCodes.SUCCESS != Transport.initialize (initArgs, rssl_err)) {
			LOG.error ("Transport.initialize: { \"rsslErrorId\": {}, \"sysError\": {}, \"text\": \"{}\" }", rssl_err.errorId(), rssl_err.sysError(), rssl_err.text());
			return false;
		}

		LOG.trace ("UPA initialization complete.");
		return true;
	}

	public void Close() {
		LOG.trace ("Uninitializing UPA.");
		Transport.uninitialize();
	}

	public boolean VerifyVersion() {
		final LibraryVersionInfo version_info = Transport.queryVersion();
		LOG.info ("LibraryVersionInfo: { \"productDate\": \"{}\", \"productInternalVersion\": \"{}\", \"productVersion\": \"{}\" }", version_info.productDate(), version_info.productInternalVersion(), version_info.productVersion());
		return true;
	}
}

/* eof */
