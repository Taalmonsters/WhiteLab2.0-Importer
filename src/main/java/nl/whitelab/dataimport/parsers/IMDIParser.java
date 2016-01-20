package nl.whitelab.dataimport.parsers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.zip.ZipEntry;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import nl.whitelab.dataimport.neo4j.Corpus;
import nl.whitelab.dataimport.neo4j.NodeLabel;
import nl.whitelab.dataimport.neo4j.LinkLabel;
import nl.whitelab.dataimport.util.ProgressMonitor;

public class IMDIParser extends MetadataParser {
//	private long tagCount = 0;

	public IMDIParser(Corpus corpus, ProgressMonitor monitor) {
		super(corpus, monitor);
	}

	@Override
	public void parseWithFields(String collection, String document, long documentId) throws XMLStreamException, IOException {
//		reset();
		
		if (zipFile != null) {
			ZipEntry e = zipFile.getEntry(metadataPathInZip + document + metadataExtension);
			if (e == null)
				e = zipFile.getEntry(metadataPathInZip + collection + "/" + document + metadataExtension);
			if (e == null) {
				System.err.println("*** ERROR, metadata entry not found: " + metadataPathInZip + document + metadataExtension);
				return;
			}
			
			lastFileSize = e.getCompressedSize();
			
			XMLEventReader reader = XML_INPUT_FACTORY.createXMLEventReader(zipFile.getInputStream(e));
			
	        try {
	    		LinkedList<String> tags = new LinkedList<String>();
	            StringBuilder textBuffer = new StringBuilder();
	            StartElement start = null;
	            boolean insideActor = false;
	            
	            while (reader.hasNext()) {
	            	XMLEvent event = (XMLEvent) reader.next();
	                if (event.isStartElement()) {
//	                	tagCount++;
	                	start = event.asStartElement();
	                	String tag = start.getName().getLocalPart();
	                	if (tag.equals("Key")) {
	                		@SuppressWarnings("unchecked")
							Iterator<Attribute> attrs = (Iterator<Attribute>) start.getAttributes();
	                		while (attrs.hasNext()) {
	                			Attribute attr = attrs.next();
	                			if (attr.getName().toString().equals("Name"))
	                				tag = attr.getValue();
	                		}
	                	}
	                	if (tag.equals("Actor"))
	                		insideActor = true;
	                	tags.add(tag);
	                } else if (event.isEndElement()) {
	                    String tag = tags.pollLast();
	                    String parent = tags.peekLast();
	                    if (parent != null && parent.equals("Keys")) {
	                    	String sub = tags.pollLast();
	                    	parent = tags.peekLast();
	                    	tags.add(sub);
	                    }
	                	if (tag.equals("Actor")) {
	                		insideActor = false;
	                	} else {
		                    String value = "";
		                    if (textBuffer.length() > 0)
		                    	value = textBuffer.toString().trim();
		                    Map<String,Object> groupProperties = metadataFields.getActorGroupProperties(tag);
		                    if (groupProperties == null)
			                    groupProperties = metadataFields.getMetadatumGroupProperties(tag);
	                		if (groupProperties != null && value.length() > 0 && !value.equals("unknown") && !value.equals("Unspecified")) {
	                			if (insideActor)
	                				tag = "Actor"+tag;

		                    	String nodeLabel = parent+":"+tag;
	                    	    Map<String, Object> properties = new HashMap<String, Object>();
	                    		properties.put("label", nodeLabel);
	                    		properties.put("group", parent);
	                    		properties.put("key", tag);
	                    		Long nodeId = nodeCreator.createUniqueIndexedNode(NodeLabel.Metadatum, properties, "label", corpusLabel, null);
	                    	    properties = new HashMap<String, Object>();
	                    		properties.put("value", value);
		            			linkCreator.createUniqueIndexedLink(documentId, nodeId, LinkLabel.HAS_METADATUM, properties, "value");
	                		}
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
	
//	private void reset() {
//		tagCount = 0;
//	}

}
