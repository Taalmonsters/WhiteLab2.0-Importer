package nl.whitelab.dataimport.util;

import java.io.File;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

import nl.whitelab.dataimport.neo4j.Corpus;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

public class ArchiveReader {
	
	private final Corpus corpus;
	
	private static Constructor<InputStream> ctorGzip;
	private static Constructor<InputStream> ctorTar;
	private static Method methodGetNextTarEntry;
	private static Method methodGetName;

	@SuppressWarnings("unchecked")
	public ArchiveReader(Corpus c) {
		corpus = c;
		Class<?> gzipClass = GzipCompressorInputStream.class;
		Class<?> tarClass = TarArchiveInputStream.class;
		Class<?> entryClass = TarArchiveEntry.class;
		try {
			ctorGzip = (Constructor<InputStream>) gzipClass.getConstructor(InputStream.class);
			ctorTar = (Constructor<InputStream>) tarClass.getConstructor(InputStream.class);
			methodGetNextTarEntry = tarClass.getMethod("getNextTarEntry");
			methodGetName = entryClass.getMethod("getName");
		} catch (NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
		}
	}

	public void processTarGzip(String collection, long collectionId, File file, InputStream tarGzipStream) {
		try {
			InputStream unzipped = ctorGzip.newInstance(tarGzipStream);
			processTar(collection, collectionId, file, unzipped);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void processTar(String collection, long collectionId, File file, InputStream tarStream) throws IOException {
		try {
			InputStream untarred = ctorTar.newInstance(tarStream);
			InputStream uncloseableInputStream = new FilterInputStream(untarred) {
				@Override
				public void close() throws IOException {
					// Don't close!
					// (when Reader is GC'ed, closes stream prematurely..?)
				}
			};
			try {
				Object tarEntry = methodGetNextTarEntry.invoke(untarred);
				while (tarEntry != null) {
					String filePath = (String) methodGetName.invoke(tarEntry);
					corpus.handleFile(collection, collectionId, file, filePath, uncloseableInputStream);
					tarEntry = methodGetNextTarEntry.invoke(untarred);
				}
			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				untarred.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
