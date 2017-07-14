package utilities;

import java.io.PrintStream;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.org.joda.time.DateTime;

/**
 * Some utilities that may be useful
 * 
 * @author emma
 *
 */
public class Util {

	public static void dumpConfigurations(Configuration conf, PrintStream ps) {

		Map<String, String> sortedConfigMap = new TreeMap<String, String>();

		for (Map.Entry<String, String> entry : conf) {
			sortedConfigMap.put(entry.getKey(), entry.getValue());
		}
		ps.println("***************** configurations ***************");
		for (Map.Entry<String, String> entry : sortedConfigMap.entrySet()) {
			ps.format("%s=%s\n", entry.getKey(), entry.getValue());
		}
		ps.println("***************** configurations ***************");
	}

	public static String getDateStamp() {

		String dateStamp = DateTime.now().toLocalDateTime().toString().replaceAll("-", "_");
		return "_" + dateStamp;

	}
}
