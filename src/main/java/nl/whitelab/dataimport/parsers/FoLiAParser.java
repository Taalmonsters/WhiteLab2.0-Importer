package nl.whitelab.dataimport.parsers;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import nl.whitelab.dataimport.neo4j.LinkCreator;
import nl.whitelab.dataimport.neo4j.NodeCreator;
import nl.whitelab.dataimport.neo4j.NodeLabel;
import nl.whitelab.dataimport.neo4j.LinkLabel;
import nl.whitelab.dataimport.util.ProgressMonitor;

public class FoLiAParser extends ContentParser {
    
	private Object2ObjectOpenHashMap<String,Long> sentenceData = new Object2ObjectOpenHashMap<String,Long>();

    private long documentId = (long) -1;
    private Long posId = (long) -1;
    private Integer tokenIndex = 0;
    private String paragraphType = null;
	private boolean newParagraph = false;
	private boolean newSentence = false;
	private String speaker = null;

	public FoLiAParser(String c, NodeCreator nc, LinkCreator lc, ProgressMonitor m) {
		super(c,nc,lc,m);
	}

	@SuppressWarnings("unchecked")
	public Integer parse(Long did, String collection, InputStream is) throws XMLStreamException, IOException {
		collectionTitle = collection;
		reset();
		documentId = did;

        XMLEventReader reader = XML_INPUT_FACTORY.createXMLEventReader(is);

        LinkedList<String> elementStack = new LinkedList<String>();
        StringBuilder textBuffer = new StringBuilder();
        StartElement start = null;
        Long prevNodeId = null;
        String xmlid = null;
        String beginTime = null;
        String endTime = null;
        Long parStartId = (long) -1;
        Long senStartId = (long) -1;
        Long tokenId = (long) -1;
        Integer senStartIndex = 1;
        boolean insideTimeSegment = false;
        boolean hasPhonetic = false;
        boolean hasPos = false;
        boolean hasLemma = false;

        while (reader.hasNext()) {
        	XMLEvent event = (XMLEvent) reader.next();
            if (event.isStartElement()) {
            	start = event.asStartElement();
            	String tag = start.getName().getLocalPart();
            	if (tag.equals("p") || tag.equals("head") || tag.equals("event")) {
            		paragraphType = tag;
            		newParagraph = true;
            	} else if (tag.equals("s")) {
            		speaker = getAttributeValue(start.getAttributes(), "speaker");
            		newSentence = true;
            	} else if (tag.equals("w")) {
            		xmlid = getAttributeValue(start.getAttributes(), "id");
            		if (xmlid != null) {
            			hasPos = false;
            	        hasLemma = false;
            			tokenIndex++;
            			Map<String,Object> props = new HashMap<String, Object>();
                		props.put("token_index", tokenIndex);
                		props.put("xmlid", xmlid);
                		tokenId = nodeCreator.createNonUniqueNonIndexedNode(NodeLabel.WordToken, props);
                		linkCreator.createNonUniqueNonIndexedLink(documentId, tokenId, LinkLabel.HAS_TOKEN, new HashMap<String, Object>());
            			sentenceData.put(xmlid, tokenId);
            			
            			if (prevNodeId != null)
            				linkCreator.createNonUniqueNonIndexedLink(prevNodeId, tokenId, LinkLabel.NEXT, new HashMap<String, Object>());
            			
            			prevNodeId = tokenId;
            			
            			if (newParagraph) {
            				Map<String,Object> parStartProps = new HashMap<String, Object>();
            				parStartProps.put("paragraph_type", paragraphType);
                    		parStartId = nodeCreator.createUniqueNonIndexedNode(NodeLabel.ParagraphStart, parStartProps, "paragraph_type");
                    		linkCreator.createNonUniqueNonIndexedLink(parStartId, tokenId, LinkLabel.STARTS_AT, new HashMap<String, Object>());
                    		newParagraph = false;
            			}
            			
            			if (newSentence) {
                    		senStartIndex = tokenIndex;
            				Map<String,Object> senStartProps = new HashMap<String, Object>();
            				senStartProps.put("start_xmlid", xmlid);
            				senStartProps.put("start_index", senStartIndex);
        					senStartId = nodeCreator.createUniqueNonIndexedNode(NodeLabel.Sentence, senStartProps, "start_xmlid");

            				Map<String,Object> properties = new HashMap<String, Object>();
            				if (speaker != null)
                				properties.put("actor_code", speaker);
            				
                    		linkCreator.createNonUniqueNonIndexedLink(senStartId, tokenId, LinkLabel.STARTS_AT, properties);
                    		linkCreator.createUniqueNonIndexedLink(senStartId, documentId, LinkLabel.OCCURS_IN, properties, null);
                    		newSentence = false;
            			}
            			
            		}
            	} else if (tag.equals("lemma")) {
        			String lemma = getAttributeValue(start.getAttributes(), "class");
        			String set = getAttributeValue(start.getAttributes(), "set");
        			if (xmlid != null && lemma != null && (set == null || set.contains("frog")) && !hasLemma) {
        				hasLemma = true;
        				addTokenAnnotation(xmlid, NodeLabel.Lemma, LinkLabel.HAS_LEMMA, lemma);
        			}
            	} else if (tag.equals("pos")) {
            		String poshead = getAttributeValue(start.getAttributes(), "head");
        			String pos = getAttributeValue(start.getAttributes(), "class");
        			if (poshead == null)
        				poshead = pos.replaceFirst("\\(.*\\)", "");
        			String set = getAttributeValue(start.getAttributes(), "set");
        			if ((set == null || set.contains("frog")) && !hasPos) {
        				hasPos = true;
        				posId = addTokenAnnotation(xmlid, NodeLabel.PosTag, LinkLabel.HAS_POS_TAG, pos);
        				Map<String, Object> properties = new HashMap<String, Object>();
        				properties.put("label", poshead);
        				addUniqueAnnotation(posId, NodeLabel.PosHead, LinkLabel.HAS_HEAD, "label", properties);
        			}
            	} else if (tag.equals("feat")) {
            		if (posId > -1) {
            			String value = getAttributeValue(start.getAttributes(), "class");
            			String key = getAttributeValue(start.getAttributes(), "subset");
        				Map<String, Object> properties = new HashMap<String, Object>();
        				properties.put("label", key+"="+value);
        				properties.put("key", key);
        				properties.put("value", value);
        				addUniqueAnnotation(posId, NodeLabel.PosFeature, LinkLabel.HAS_FEATURE, "label", properties);
            		}
            	} else if (tag.equals("timesegment")) {
            		insideTimeSegment = true;
            		beginTime = getAttributeValue(start.getAttributes(), "begintime");
            		endTime = getAttributeValue(start.getAttributes(), "endtime");
            	} else if (tag.equals("wref") && insideTimeSegment && beginTime != null && endTime != null) {
            		String xid = getAttributeValue(start.getAttributes(), "id");
            		if (sentenceData.containsKey(xid)) {
            			nodeCreator.setNodeProperty(sentenceData.get(xid), "begin_time", beginTime);
            			nodeCreator.setNodeProperty(sentenceData.get(xid), "end_time", endTime);
            		}
            	}
            	elementStack.add(tag);
            } else if (event.isEndElement()) {
            	String tag = elementStack.pollLast();
            	String parent = elementStack.peekLast();
            	if (tag.equals("t") && parent.equals("w") && xmlid != null) {
    				addTokenAnnotation(xmlid, NodeLabel.WordType, LinkLabel.HAS_TYPE, textBuffer.toString().trim());
				} else if (tag.equals("ph") && parent.equals("w") && xmlid != null) {
					addTokenAnnotation(xmlid, NodeLabel.Phonetic, LinkLabel.HAS_PHONETIC, textBuffer.toString().trim());
					hasPhonetic = true;
				} else if (tag.equals("w")) {
					if (!hasPhonetic)
						addTokenAnnotation(xmlid, NodeLabel.Phonetic, LinkLabel.HAS_PHONETIC, "");
					hasPhonetic = false;
					posId = (long) -1;
					xmlid = null;
				} else if (tag.equals("s")) {
					sentenceData = new Object2ObjectOpenHashMap<String,Long>();
            		if (senStartId > -1 && tokenId > -1) {
            			// Add token count to sentence
            			Integer senTokenCount = tokenIndex - (senStartIndex - 1);
            			nodeCreator.setNodeProperty(senStartId, "end_index", tokenIndex);
            			nodeCreator.setNodeProperty(senStartId, "token_count", senTokenCount);
                		linkCreator.createNonUniqueNonIndexedLink(senStartId, tokenId, LinkLabel.ENDS_AT, new HashMap<String, Object>());
            		}
				} else if (tag.equals("timesegment")) {
					insideTimeSegment = false;
					beginTime = null;
					endTime = null;
				} else if (tag.equals("p") || tag.equals("head") || tag.equals("event")) {
            		if (parStartId > -1 && tokenId > -1) {
                		linkCreator.createNonUniqueNonIndexedLink(parStartId, tokenId, LinkLabel.ENDS_AT, new HashMap<String, Object>());
            		}
				}
            	textBuffer = new StringBuilder();
            } else if (event.isCharacters()) {
            	textBuffer.append(event.asCharacters().getData());
            }
        }

        reader.close();
//        in.close();

        return tokenIndex;
	}

	private String getAttributeValue(Iterator<Attribute> atts, String str) {
		while (atts.hasNext()) {
			Attribute att = (Attribute) atts.next();
			if (att.getName().getLocalPart().equals(str)) {
				return att.getValue();
			}
		}
		return null;
	}

	private void reset() {
		sentenceData = new Object2ObjectOpenHashMap<String,Long>();
		tokenIndex = 0;
	    speaker = null;
	}
	
	private Long addTokenAnnotation(String tokenId, NodeLabel nodeLabel, LinkLabel linkLabel, String value) {
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put("label", value);
		return addAnnotation(sentenceData.get(tokenId), nodeLabel, linkLabel, "label", properties);
	}
	
	private Long addAnnotation(Long nodeId, NodeLabel nodeLabel, LinkLabel linkLabel, String propLabel, Map<String,Object> properties) {
		Long id = nodeCreator.createUniqueIndexedNode(nodeLabel, properties, propLabel, corpusLabel, collectionTitle);
		linkCreator.createNonUniqueNonIndexedLink(nodeId, id, linkLabel, new HashMap<String,Object>());
//		linkCreator.createUniqueNonIndexedLink(id, documentId, LinkLabel.OCCURS_IN, new HashMap<String,Object>(), null);
		return id;
	}
	
	private Long addUniqueAnnotation(Long nodeId, NodeLabel nodeLabel, LinkLabel linkLabel, String propLabel, Map<String,Object> properties) {
		Long id = nodeCreator.createUniqueIndexedNode(nodeLabel, properties, propLabel, corpusLabel, collectionTitle);
		linkCreator.createUniqueNonIndexedLink(nodeId, id, linkLabel, new HashMap<String,Object>(), propLabel);
//		linkCreator.createUniqueNonIndexedLink(id, documentId, LinkLabel.OCCURS_IN, new HashMap<String,Object>(), null);
		return id;
	}

}
