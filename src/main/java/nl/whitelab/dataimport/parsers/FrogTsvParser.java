package nl.whitelab.dataimport.parsers;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import nl.whitelab.dataimport.neo4j.LinkCreator;
import nl.whitelab.dataimport.neo4j.NodeCreator;
import nl.whitelab.dataimport.neo4j.NodeLabel;
import nl.whitelab.dataimport.neo4j.LinkLabel;
import nl.whitelab.dataimport.util.ProgressMonitor;

public class FrogTsvParser extends ContentParser {
    
	private Object2ObjectOpenHashMap<String,Long> sentenceData = new Object2ObjectOpenHashMap<String,Long>();

    private long documentId = (long) -1;
    private Long posId = (long) -1;
    private Integer tokenIndex = 0;

	public FrogTsvParser(String c, NodeCreator nc, LinkCreator lc, ProgressMonitor m) {
		super(c,nc,lc,m);
	}

	public Integer parse(Long did, String collection, InputStream is) throws NumberFormatException, IOException {
		collectionTitle = collection;
		reset();
		documentId = did;

        Long prevNodeId = null;
        Long parStartId = (long) -1;
        Long senStartId = (long) -1;
        Long tokenId = (long) -1;
        Integer senStartIndex = 1;
        
        Integer s = 1;
        
        String str = "";
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        if (is != null) {                            
            while ((str = reader.readLine()) != null) {
            	if (str.trim().length() == 0) {
            		s++;
            	} else {
            		String[] parts = str.trim().split("\t");
            		Integer tokenIndex = Integer.parseInt(parts[0]);
            		String token = parts[1];
            		String lemma = parts[2];
            		String pos = parts[4];
            		String xmlid = documentId+".p.1.s."+String.valueOf(s)+".w."+String.valueOf(tokenIndex);
            		
            		Map<String,Object> props = new HashMap<String, Object>();
            		props.put("token_index", tokenIndex);
            		props.put("xmlid", xmlid);
            		tokenId = nodeCreator.createNonUniqueNonIndexedNode(NodeLabel.WordToken, props);
            		linkCreator.createNonUniqueNonIndexedLink(documentId, tokenId, LinkLabel.HAS_TOKEN, new HashMap<String, Object>());
        			sentenceData.put(xmlid, tokenId);
        			
        			if (prevNodeId != null)
        				linkCreator.createNonUniqueNonIndexedLink(prevNodeId, tokenId, LinkLabel.NEXT, new HashMap<String, Object>());
        			
        			prevNodeId = tokenId;
        			
        			if (s == 1 && tokenIndex == 1) {
        				Map<String,Object> parStartProps = new HashMap<String, Object>();
        				parStartProps.put("paragraph_type", "p");
                		parStartId = nodeCreator.createUniqueNonIndexedNode(NodeLabel.ParagraphStart, parStartProps, "paragraph_type");
                		linkCreator.createNonUniqueNonIndexedLink(parStartId, tokenId, LinkLabel.STARTS_AT, new HashMap<String, Object>());
        			}
        			
        			if (tokenIndex == 1) {
                		senStartIndex = tokenIndex;
        				Map<String,Object> senStartProps = new HashMap<String, Object>();
        				senStartProps.put("start_xmlid", xmlid);
        				senStartProps.put("start_index", senStartIndex);
    					senStartId = nodeCreator.createUniqueNonIndexedNode(NodeLabel.Sentence, senStartProps, "start_xmlid");

        				Map<String,Object> properties = new HashMap<String, Object>();
        				
                		linkCreator.createNonUniqueNonIndexedLink(senStartId, tokenId, LinkLabel.STARTS_AT, properties);
                		linkCreator.createUniqueNonIndexedLink(senStartId, documentId, LinkLabel.OCCURS_IN, properties, null);
        			}
        			
        			addTokenAnnotation(xmlid, NodeLabel.WordType, LinkLabel.HAS_TYPE, token);
        			
        			addTokenAnnotation(xmlid, NodeLabel.Lemma, LinkLabel.HAS_LEMMA, lemma);
        			
        			String poshead = pos.split("(")[0];
    				posId = addTokenAnnotation(xmlid, NodeLabel.PosTag, LinkLabel.HAS_POS_TAG, pos);
    				Map<String, Object> properties = new HashMap<String, Object>();
    				properties.put("label", poshead);
    				addUniqueAnnotation(posId, NodeLabel.PosHead, LinkLabel.HAS_HEAD, "label", properties);
            	}
            }                
        }

        reader.close();
//        is.close();

        return tokenIndex;
	}

	private void reset() {
		sentenceData = new Object2ObjectOpenHashMap<String,Long>();
		tokenIndex = 0;
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
