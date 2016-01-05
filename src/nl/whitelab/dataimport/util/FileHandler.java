package nl.whitelab.dataimport.util;

import java.io.IOException;
import java.io.InputStream;

public interface FileHandler {
	void handle(String filePath, InputStream contents) throws IOException;
}
