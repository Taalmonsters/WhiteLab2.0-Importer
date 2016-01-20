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

//import com.google.common.base.CaseFormat;


import nl.whitelab.dataimport.neo4j.Corpus;
import nl.whitelab.dataimport.neo4j.NodeLabel;
import nl.whitelab.dataimport.neo4j.LinkLabel;
import nl.whitelab.dataimport.util.ProgressMonitor;

public class SoNaRCMDIParser extends MetadataParser {
	protected Corpus corpus;

	public SoNaRCMDIParser(Corpus corpus, ProgressMonitor monitor) {
		super(corpus, monitor);
		this.corpus = corpus;
	}

	@Override
	public void parseWithFields(String collection, String document, long documentId) throws XMLStreamException, IOException {
//		long tagCount = 0;
		reset();

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
	            Map<String,Object> currentActor = new HashMap<String,Object>();
	
	            while (reader.hasNext()) {
	            	XMLEvent event = (XMLEvent) reader.next();
	                if (event.isStartElement()) {
	                	start = event.asStartElement();
	                	String tag = start.getName().getLocalPart();
	                	tags.add(tag);
	                } else if (event.isEndElement()) {
	                    String tag = tags.pollLast();
	                    String parent = tags.pollLast();
	                    String grandParent = tags.peekLast();
	                	tags.add(parent);
	                    String value = "";
	                    if (textBuffer.length() > 0)
	                    	value = textBuffer.toString().trim();
	                    
	                    if (value.length() > 0) {
	                    	if (grandParent.equals("Author") || parent.equals("Author")) {
	                    		Object v = convertValue(value);
	                    		if (v != null)
            						currentActor.put(tag, v);
//	                    			currentActor.put(CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, tag), v);
	                    	} else if (!parent.contains("-") && !tag.contains("-")) {
	                    		if (tag.startsWith(parent))
		                    		tag = tag.replaceFirst(parent, "");
	                    		Object v = convertValue(value);
	                    		if (v != null)
	                    			addMetadatum(documentId, parent, tag, v);
	                    	}
	                    }
	                    
	                    if (tag.equals("Author") && currentActor.size() > 0)
	                    	addActor(documentId, currentActor);
	                    
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
	
	protected void addActor(long documentId, Map<String, Object> currentActor) {
		String actorCode = corpus.getNewActorCode();
		if (actorCode != null && currentActor.size() > 0) {
			currentActor.put("Type", "author");
			for (String property : currentActor.keySet()) {
				Map<String, Object> actorMetaProperties = new HashMap<String, Object>();
				actorMetaProperties.put("label", "Actor"+property);
				actorMetaProperties.put("group", "Actor");
				actorMetaProperties.put("key", property);
				Long metaNodeId = nodeCreator.createUniqueIndexedNode(NodeLabel.Metadatum, actorMetaProperties, "label", corpusLabel, null);
				Map<String, Object> actorMetaValues = new HashMap<String, Object>();
				actorMetaValues.put("value", currentActor.get(property));
				actorMetaValues.put("actor_code", actorCode);
				linkCreator.createUniqueIndexedLink(documentId, metaNodeId, LinkLabel.HAS_METADATUM, actorMetaValues, "actor_code");
			}
		}
	}

	@Override
	public void parseWithoutFields(String collection, String document, long documentId) throws XMLStreamException, IOException {
		parseWithFields(collection, document, documentId);
	}
	
	private void reset() {
//		tagCount = 0;
	}

}
