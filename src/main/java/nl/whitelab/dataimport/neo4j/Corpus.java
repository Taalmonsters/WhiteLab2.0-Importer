package nl.whitelab.dataimport.neo4j;

import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.FileVisitResult;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import javax.xml.stream.XMLStreamException;

//import nl.whitelab.dataimport.parsers.CGNIMDIParser;
//import nl.whitelab.dataimport.parsers.SoNaRCMDIParser;
import nl.whitelab.dataimport.parsers.ContentParser;
//import nl.whitelab.dataimport.parsers.FoLiAParser;
//import nl.whitelab.dataimport.parsers.IMDIParser;
import nl.whitelab.dataimport.parsers.MetadataParser;
import nl.whitelab.dataimport.util.ArchiveReader;
import nl.whitelab.dataimport.util.ProgressMonitor;

public class Corpus {
	
	protected static final FileVisitResult CONTINUE = FileVisitResult.CONTINUE;

    private long corpusId = -1;
	private final String title;
	private final String displayName;
	private final String baseDir;
	private final String corpusLabel;
	
	public final String metadataFormat;
	private final String[] audioFormat;
	private final String audioWebFormat;
	private final String audioExportFormat;
	
	public final String metadataZipFile;
	public final String metadataPathInZip;
	public final String metadataExtension;
	public final MetadataFields metadataFields;

	private boolean hasAudio;

    private final NodeCreator nodeCreator;
    private final LinkCreator linkCreator;
    private final ContentParser contentParser;
    private final MetadataParser metaParser;
    private final ProgressMonitor monitor;

    private long badFileCount = 0;
    private long goodFileCount = 0;
    private long totalTokenCount = 0;
    private long totalActorCount = 0;
    private boolean includeMeta = true;

    private Map<String,Integer> collectionTokenCounts = new HashMap<String,Integer>();
    private Map<String,Integer> collectionDocumentCounts = new HashMap<String,Integer>();
    private Object2LongOpenHashMap<String> collections = new Object2LongOpenHashMap<String>();
    private Object2ObjectOpenHashMap<String,Map<String,Object>> documents = new Object2ObjectOpenHashMap<String,Map<String,Object>>();

	public Corpus(String label, String fullPath, NodeCreator nc, LinkCreator lc, ProgressMonitor m) throws ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		monitor = m;
		monitor.log("Loading corpus from: "+fullPath);
		corpusLabel = label;
		baseDir = fullPath;
		hasAudio = false;
		Properties properties = loadProperties(fullPath+"/indexer.properties");

		title = properties.getProperty("title");
		monitor.log("Title: "+title);
		displayName = properties.getProperty("displayName");
		metadataFormat = properties.getProperty("metadataFormat").toLowerCase();
		
		if (properties.containsKey("audioFormat")) {
			audioFormat = properties.getProperty("audioFormat").toLowerCase().split(",");
			hasAudio = true;
		} else
			audioFormat = null;
		
		if (properties.containsKey("audioWebFormat"))
			audioWebFormat = properties.getProperty("audioWebFormat").toLowerCase();
		else
			audioWebFormat = null;
		if (properties.containsKey("audioExportFormat"))
			audioExportFormat = properties.getProperty("audioExportFormat").toLowerCase();
		else
			audioExportFormat = null;
		metadataZipFile = properties.getProperty("metadataZipFile");
		metadataPathInZip = properties.getProperty("metadataPathInZip");
		metadataExtension = properties.getProperty("metadataExtension");
		metadataFields = new MetadataFields(baseDir+"/metadata/indexmetadata.json");
		
		nodeCreator = nc;
		linkCreator = lc;

		contentParser = getContentParser(properties);
		metaParser = getMetadataParser(properties);
		
	}
	
	private ContentParser getContentParser(Properties properties) throws ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		Class<?> clazz = Class.forName("nl.whitelab.dataimport.parsers.FoLiAParser");
		if (properties.containsKey("contentParserClass"))
			clazz = Class.forName("nl.whitelab.dataimport.parsers."+properties.getProperty("contentParserClass"));
		Constructor<?> constructor = clazz.getConstructor(String.class, NodeCreator.class, LinkCreator.class, ProgressMonitor.class);
		return (ContentParser) constructor.newInstance(corpusLabel, nodeCreator, linkCreator, monitor);
	}
	
	private MetadataParser getMetadataParser(Properties properties) throws ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		Class<?> clazz = Class.forName("nl.whitelab.dataimport.parsers.CMDIParser");
		if (properties.containsKey("metadataParserClass"))
			clazz = Class.forName("nl.whitelab.dataimport.parsers."+properties.getProperty("metadataParserClass"));
		Constructor<?> constructor = clazz.getConstructor(Corpus.class, ProgressMonitor.class);
		return (MetadataParser) constructor.newInstance(this, monitor);
	}
	
	public String getTitle() {
		return title;
	}
	
	private Properties loadProperties(String propFileName) {
		monitor.log("Loading properties from: "+propFileName);
		Properties properties = new Properties();
		
		try(BufferedReader br = new BufferedReader(new FileReader(propFileName))) {
		    for(String line; (line = br.readLine()) != null; ) {
		        if (line.length() > 0 && !line.startsWith("#")) {
		        	String[] parts = line.split("=");
		        	properties.setProperty(parts[0], parts[1]);
		        }
		    }
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		return properties;
	}
	
	public void process() throws IOException {
		monitor.log("Processing corpus: "+title);
		if (corpusId == -1) {
			Map<String, Object> properties = new HashMap<String, Object>();
			properties.put("title", title);
			properties.put("display_name", displayName);
			properties.put("label", corpusLabel);
			properties.put("content_viewable", true);
			properties.put("has_audio", hasAudio);
			properties.put("metadata_format", metadataFormat);
			properties.put("metadata_zip_file", metadataZipFile);
			properties.put("metadata_path_in_zip", metadataPathInZip);
			properties.put("metadata_extension", metadataExtension);
			properties.put("metadata_fields_file", "metadata/indexmetadata.json");
			if (audioWebFormat != null)
				properties.put("audio_web_format", audioWebFormat);
			if (audioExportFormat != null)
				properties.put("audio_export_format", audioExportFormat);
			corpusId = nodeCreator.createUniqueIndexedNode(NodeLabel.Corpus, properties, "label", corpusLabel, null);
		}
		
		includeMeta = true;
		processCorpusContent();

		Object tc = nodeCreator.getNodeProperty(corpusId, "token_count");
		if (tc == null)
			nodeCreator.setNodeProperty(corpusId, "token_count", totalTokenCount);
		else
			nodeCreator.setNodeProperty(corpusId, "token_count", totalTokenCount + (Long) tc);
		
		Object dc = nodeCreator.getNodeProperty(corpusId, "document_count");
		if (dc == null)
			nodeCreator.setNodeProperty(corpusId, "document_count", goodFileCount);
		else
			nodeCreator.setNodeProperty(corpusId, "document_count", goodFileCount + (Long) dc);
		
		Object cc = nodeCreator.getNodeProperty(corpusId, "collection_count");
		if (cc == null)
			nodeCreator.setNodeProperty(corpusId, "collection_count", collections.size());
		else
			nodeCreator.setNodeProperty(corpusId, "collection_count", collections.size() + (Integer) cc);
		
		Iterator<String> it = collectionTokenCounts.keySet().iterator();
		while (it.hasNext()) {
			String collection = it.next();
			Long id = collections.get(collection);
			
			Object ctc = nodeCreator.getNodeProperty(id, "token_count");
			if (ctc == null)
				nodeCreator.setNodeProperty(id, "token_count", collectionTokenCounts.get(collection));
			else
				nodeCreator.setNodeProperty(id, "token_count", collectionTokenCounts.get(collection) + (Long) ctc);
			
			Object cdc = nodeCreator.getNodeProperty(corpusId, "document_count");
			if (cdc == null)
				nodeCreator.setNodeProperty(id, "document_count", collectionDocumentCounts.get(collection));
			else
				nodeCreator.setNodeProperty(id, "document_count", (Long) cdc);
		}
		
		monitor.log("Finished processing corpus :"+title);
		monitor.log("Processed "+goodFileCount+" files ("+badFileCount+" failed)");
	}
	
	private void processCorpusContent() throws IOException {
		monitor.log("Processing corpus content...");
		File contentDir = new File(baseDir+"/input/");
		for (final File file : contentDir.listFiles()) {
			String fileName = file.getName();
			if (file.isFile() && file.canRead() && fileName.endsWith(".tar.gz")) {
				final String collection = fileName.replace(".tar.gz", "");
				if (!collections.containsKey(collection)) {
					Map<String, Object> properties = new HashMap<String, Object>();
					properties.put("title", collection);
					properties.put("display_name", collection);
					final long collectionId = nodeCreator.createUniqueIndexedNode(NodeLabel.Collection, properties, "title", corpusLabel, null);
					linkCreator.createNonUniqueNonIndexedLink(corpusId, collectionId, LinkLabel.HAS_COLLECTION, new HashMap<String, Object>());
					collections.put(collection, collectionId);
					ArchiveReader reader = new ArchiveReader(this);
					reader.processTarGzip(collection, collectionId, file, new FileInputStream(file.getAbsolutePath()));
				}
			}
			else if (!file.isFile())
				monitor.log("*** Warning, not a file: "+fileName);
			else if (!file.canRead())
				monitor.log("*** Warning, file not readable: "+fileName);
		}
		
		monitor.log("processCorpusContent");
	}

	public void handleFile(String collection, long collectionId, File file, String filePath, InputStream contents) throws IOException {
		try {
			File f = new File(file.getCanonicalPath() + File.separator + filePath);
			String fn = f.getName();
			if (fn.endsWith("folia.xml")) {
				String entryName = file.getCanonicalPath() + File.separator + filePath;
        		String[] parts = entryName.split("/");
        		String document = parts[parts.length - 1].replace(".folia.xml", "");
        		
				processDocument(collection, collectionId, document, contents);
				goodFileCount++;
				if (!collectionDocumentCounts.containsKey(collection))
					collectionDocumentCounts.put(collection, 0);
				collectionDocumentCounts.put(collection, collectionDocumentCounts.get(collection) + 1);
			}
		} catch (Exception e) {
			badFileCount++;
			monitor.log("*** Error indexing file: " + file.getCanonicalPath() + File.separator + filePath);
			e.printStackTrace();
		}
	}
	
	private void processDocument(String collection, long collectionId, String document, InputStream contents) {
		try {
			long fileSize = contents.available();
    		Map<String, Object> properties = new HashMap<String, Object>();
			properties.put("xmlid", document);
			properties.put("corpus_label", corpusLabel);
			
			if (hasAudio)
				for (String format : audioFormat)
					properties.put(format, format+"/"+document+"."+format);

			long documentId = nodeCreator.createUniqueIndexedNode(NodeLabel.Document, properties, "xmlid", corpusLabel, null);
			linkCreator.createNonUniqueNonIndexedLink(collectionId, documentId, LinkLabel.HAS_DOCUMENT, new HashMap<String, Object>());

			if (includeMeta)
			    metaParser.parse(collection, document, documentId);
			
			int token_count = contentParser.parse(documentId, collection, contents);
			nodeCreator.setNodeProperty(documentId, "token_count", token_count);
			
			monitor.update(fileSize,1,token_count);
			
			totalTokenCount = totalTokenCount + token_count;
			if (!collectionTokenCounts.containsKey(collection))
				collectionTokenCounts.put(collection, 0);
			collectionTokenCounts.put(collection, collectionTokenCounts.get(collection) + token_count);
		} catch (XMLStreamException | IOException e) {
			monitor.log("Failed to process document content ("+collection+"): "+document);
			e.printStackTrace();
		}
		
		if (documents.containsKey(document))
			documents.remove(document);
	}

	public NodeCreator getNodeCreator() {
		return nodeCreator;
	}

	public LinkCreator getLinkCreator() {
		return linkCreator;
	}
	
	public String getBaseDir() {
		return baseDir;
	}
	
	public String getLabel() {
		return corpusLabel;
	}
	
	public String getNewActorCode() {
		totalActorCount++;
		return corpusLabel+String.valueOf(totalActorCount);
	}

}
