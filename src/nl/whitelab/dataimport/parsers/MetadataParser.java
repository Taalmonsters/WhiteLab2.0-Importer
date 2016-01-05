package nl.whitelab.dataimport.parsers;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipFile;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;

import org.codehaus.stax2.XMLInputFactory2;

import nl.whitelab.dataimport.neo4j.Corpus;
import nl.whitelab.dataimport.neo4j.LinkCreator;
import nl.whitelab.dataimport.neo4j.LinkLabel;
import nl.whitelab.dataimport.neo4j.MetadataFields;
import nl.whitelab.dataimport.neo4j.NodeCreator;
import nl.whitelab.dataimport.neo4j.NodeLabel;
import nl.whitelab.dataimport.util.ProgressMonitor;

public abstract class MetadataParser {
	protected static final XMLInputFactory XML_INPUT_FACTORY = XMLInputFactory2.newInstance();
	protected final String corpusLabel;
	protected final String metadataZipFile;
	protected final String metadataPathInZip;
	protected final String metadataExtension;
	protected final MetadataFields metadataFields;
	protected ZipFile zipFile;

	protected static NodeCreator nodeCreator;
	protected static LinkCreator linkCreator;
	protected final ProgressMonitor monitor;
	
	public long lastFileSize = -1;
	
	public MetadataParser(Corpus corpus, ProgressMonitor monitor) {
		corpusLabel = corpus.getLabel();
		metadataFields = corpus.metadataFields;
		metadataZipFile = corpus.getBaseDir()+"/"+corpus.metadataZipFile;
		nodeCreator = corpus.getNodeCreator();
		linkCreator = corpus.getLinkCreator();
		this.monitor = monitor;

		if (corpus.metadataPathInZip.length() > 0 && !corpus.metadataPathInZip.endsWith("/"))
			metadataPathInZip = corpus.metadataPathInZip+"/";
		else
			metadataPathInZip = corpus.metadataPathInZip;

		if (corpus.metadataExtension.length() > 0 && !corpus.metadataExtension.startsWith("."))
			metadataExtension = "."+corpus.metadataExtension;
		else
			metadataExtension = corpus.metadataExtension;

		zipFile = null;
		try {
			zipFile = new ZipFile(new File(metadataZipFile));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void parse(String collection, String document, long documentId) throws XMLStreamException, IOException {
		if (metadataFields != null)
			parseWithFields(collection, document, documentId);
		else
			parseWithoutFields(collection, document, documentId);
	}
	
	public abstract void parseWithFields(String collection, String document, long documentId) throws XMLStreamException, IOException;
	
	public abstract void parseWithoutFields(String collection, String document, long documentId) throws XMLStreamException, IOException;

	protected long addMetadatum(Long documentId, String parent, String tag, Object value) {
		String nodeLabel = parent+""+tag;
	    Map<String, Object> properties = new HashMap<String, Object>();
		properties.put("label", nodeLabel);
		properties.put("group", parent);
		properties.put("key", tag);
		Long nodeId = nodeCreator.createUniqueIndexedNode(NodeLabel.Metadatum, properties, "label", corpusLabel, null);
	    properties = new HashMap<String, Object>();
		properties.put("value", value);
		linkCreator.createUniqueIndexedLink(documentId, nodeId, LinkLabel.HAS_METADATUM, properties, "value");
		return nodeId;
	}
	
	protected Object convertValue(String value) {
		if (value.toLowerCase().equals("unspecified") || value.toLowerCase().equals("unknown") || value.toLowerCase().equals("none")) {
			return null;
		} else if (value.toLowerCase().equals("true") || value.toLowerCase().equals("false")) {
			return Boolean.parseBoolean(value.toLowerCase());
		} else if (value.matches("-?\\d+\\.\\d+?")) {
			return Double.parseDouble(value);
		} else if (value.matches("-?\\d+") && value.length() < 10) {
			return Integer.parseInt(value);
		}
		return value;
	}

}
