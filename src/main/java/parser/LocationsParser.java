package parser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class LocationsParser {

	public static void main(String[] argv) throws Exception {
		ClassLoader classloader = Thread.currentThread().getContextClassLoader();
		InputStream is = classloader.getResourceAsStream("locations.csv");
		BufferedReader reader = new BufferedReader(new InputStreamReader(is));

		try {
			String line;
			while ((line = reader.readLine()) != null) {
				String[] lineParts = line.split(",");
				String locationId = lineParts[0];
				String location = lineParts[2];
				System.out.println("locations.put(\"" + locationId + "\", " + location + ");");
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

}
