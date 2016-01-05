package nl.whitelab.dataimport.parsers;

import java.io.IOException;
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

public class CGNIMDIParser extends MetadataParser {

	public CGNIMDIParser(Corpus corpus, ProgressMonitor monitor) {
		super(corpus, monitor);
	}

	@Override
	public void parseWithFields(String collection, String document, long documentId) throws XMLStreamException, IOException {
		reset();
		
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
	            boolean insideMDGroup = false;
	            boolean insideActor = false;
	            Map<String,Object> currentActor = new HashMap<String,Object>();
	            Object languageName = null;
	            Object languageId = null;
	            Object languageDescr = null;
	            
	            while (reader.hasNext()) {
	            	XMLEvent event = (XMLEvent) reader.next();
	                if (event.isStartElement()) {
//	                	tagCount++;
	                	start = event.asStartElement();
	                	String tag = start.getName().getLocalPart();
	                	if (tag.equals("MDGroup"))
	                		insideMDGroup = true;
	                	if (tag.equals("Actor"))
	                		insideActor = true;
	                	tags.add(tag);
	                } else if (event.isEndElement()) {
	                    String tag = tags.pollLast();
	                    String parent = tags.pollLast();
	                    String grandParent = tags.peekLast();
	                	tags.add(parent);
	                    String value = "";
	                    if (textBuffer.length() > 0)
	                    	value = textBuffer.toString().trim();
	                    
	                    if (parent != null && !parent.equals("Keys") && tag != null && !tag.equals("Key")) {
	                    	if (insideMDGroup) {
	                    		if (insideActor) {
	                    			if (tag.equals("Actor")) {
	                    				addActor(documentId, currentActor);
	                    			} else if (value.length() > 0) {
	                    				if (parent.equals("Language")) {
	                    					if (tag.equals("Id"))
	                    						languageId = convertValue(value);
	                    					else if (tag.equals("Name"))
	                    						languageName = convertValue(value);
	                    					else if (tag.equals("Description"))
	                    						languageDescr = convertValue(value.replaceAll(" ", ""));
	                    					
	                    					if (languageId != null && languageName != null && languageDescr != null) {
	                    						currentActor.put((String) languageDescr, languageName);
	                    						currentActor.put(languageDescr+"Id", languageId);
	                    						languageName = null;
	                    			            languageId = null;
	                    			            languageDescr = null;
	                    					}
	                    				} else {
	                    					Object v = convertValue(value);
	                    					if (v != null)
	                    						currentActor.put(tag, v);
	                    				}
	                    			}
	                    		} else if (value.length() > 0 && !parent.equals("Actors")) {
			                    	if (grandParent.equals("Project") || grandParent.equals("Content")) {
			                    		tag = parent+tag;
			                    		parent = grandParent;
			                    	}
			                    	Object v = convertValue(value);
		                    		if (v != null)
		                    			addMetadatum(documentId, parent, tag, v);
	                    		}
	                    	} else if (value.length() > 0 && parent.equals("Session") && (tag.equals("Name") || tag.equals("Title") || tag.equals("Date"))) {
	                    		Object v = convertValue(value);
	                    		if (v != null)
	                    			addMetadatum(documentId, parent, tag, v);
	                    	}
	                    }
	                    
	                    if (tag.equals("MDGroup"))
	                		insideMDGroup = false;
	                	if (tag.equals("Actor"))
	                		insideActor = false;
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
		String actorCode = (String) currentActor.remove("Code");
		if (actorCode != null && currentActor.size() > 0) {
			currentActor.put("Type", "speaker");
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
