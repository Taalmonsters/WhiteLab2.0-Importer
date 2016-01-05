package nl.whitelab.dataimport.parsers;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.zip.ZipEntry;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import nl.whitelab.dataimport.neo4j.Corpus;
import nl.whitelab.dataimport.neo4j.NodeLabel;
import nl.whitelab.dataimport.neo4j.LinkLabel;
import nl.whitelab.dataimport.util.ProgressMonitor;

public class CMDIParser extends MetadataParser {

	public CMDIParser(Corpus corpus, ProgressMonitor monitor) {
		super(corpus, monitor);
	}

	@Override
	public void parseWithFields(String collection, String document, long documentId) throws XMLStreamException, IOException {
//		long tagCount = 0;

		if (zipFile != null) {
			String zipFileName = metadataPathInZip + document + metadataExtension;
			ZipEntry e = zipFile.getEntry(zipFileName);
			if (e == null) {
				zipFileName = metadataPathInZip + collection + "/" + document + metadataExtension;
				e = zipFile.getEntry(zipFileName);
			}
			if (e == null) {
				System.err.println("*** ERROR, metadata entry not found: " + zipFileName + "(collection: "+collection+", document: "+document+")");
				return;
			}
			
			lastFileSize = e.getCompressedSize();
	
			InputStream zis = zipFile.getInputStream(e);
			XMLEventReader reader = XML_INPUT_FACTORY.createXMLEventReader(zis);
			
	        try {
	    		LinkedList<String> tags = new LinkedList<String>();
	            StringBuilder textBuffer = new StringBuilder();
	            StartElement start = null;
	
	            while (reader.hasNext()) {
	            	XMLEvent event = (XMLEvent) reader.next();
	                if (event.isStartElement()) {
	                	start = event.asStartElement();
	                	String tag = start.getName().getLocalPart();
	                	tags.add(tag);
	                } else if (event.isEndElement()) {
	                    String tag = tags.pollLast();
	                    String parent = tags.peekLast();
	                    String value = "";
	                    Map<String,Object> groupProperties = metadataFields.getMetadatumGroupProperties(tag);
	                    if (textBuffer.length() > 0)
	                    	value = textBuffer.toString().trim();
	                    if (groupProperties != null && value.length() > 0 && !value.equals("unknown") && !value.equals("Unspecified")) {
	                    	if (tag.startsWith(parent))
	                    		tag = tag.replaceFirst(parent, "");
	                    	String nodeLabel = parent+"_"+tag;
                    	    Map<String, Object> properties = new HashMap<String, Object>();
                    		properties.put("label", nodeLabel);
                    		properties.put("group", parent);
                    		properties.put("key", tag);
                    		Long metadatumId = nodeCreator.createUniqueIndexedNode(NodeLabel.Metadatum, properties, "label", corpusLabel, null);
                    	    properties = new HashMap<String, Object>();
                    		properties.put("value", value);
	            			linkCreator.createUniqueIndexedLink(documentId, metadatumId, LinkLabel.HAS_METADATUM, properties, "value");
	                    }
	                    textBuffer = new StringBuilder();
	                } else if (event.isCharacters()) {
	                	textBuffer.append(event.asCharacters().getData());
	                }
	            }
	        } finally {
	            reader.close();
	        }
		}
	}

	@Override
	public void parseWithoutFields(String collection, String document, long documentId) throws XMLStreamException, IOException {
		// TODO Auto-generated method stub
		parseWithFields(collection, document, documentId);
	}

}
