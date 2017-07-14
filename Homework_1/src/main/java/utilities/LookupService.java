package utilities;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Convenient look up service for getting information from the cache, assumes a string token and integer value.
 *
 */
public class LookupService {

	private static final Log LOG = LogFactory.getLog(LookupService.class);

	private Map<String, Integer> lookupTable = new HashMap<String, Integer>();

	public Integer get(String key) {
		return lookupTable.get(key);
	}

	public void initialize(URI uri) throws IOException {

		File file = new File(uri.getPath());

		LOG.info("initializing cache with file: " + file);

		BufferedReader in = new BufferedReader(new FileReader(file));
		String line = null;
		String value = null;

		try {
			while ((line = in.readLine()) != null) {
				String[] token = line.toString().split("\\t");
				if (token.length == 2)
					lookupTable.put(token[0], Integer.valueOf(token[1]));
			}
		} finally {
			LOG.info("cache has: " + lookupTable.size() + " entries");
			org.apache.commons.io.IOUtils.closeQuietly(in);
		}
	}
}
