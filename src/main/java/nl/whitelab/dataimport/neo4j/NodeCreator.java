package nl.whitelab.dataimport.neo4j;

import java.util.Iterator;
import java.util.Map;

import nl.whitelab.dataimport.tools.IndexTool;
import nl.whitelab.dataimport.util.ProgressMonitor;

import org.neo4j.unsafe.batchinsert.BatchInserterIndex;

public class NodeCreator extends ElementCreator {

	public NodeCreator(IndexTool in, ProgressMonitor m) {
		super(in, m);
	}
	
	public Long getNodeId(NodeLabel label, String lexiconIdLabel, String propertyLabel, String propertyValue, BatchInserterIndex index, String corpusLabel, String collectionTitle) {
		return getElementId(label.toString(), lexiconIdLabel, propertyLabel, propertyValue, index, corpusLabel, collectionTitle);
	}
	
	public int getNodeCount() {
		return getElementCount();
	}
	
	public long getNodeCount(String label) {
		return getElementCount(label);
	}
	
	public Object getNodeProperty(Long id, String prop) {
		Map<String,Object> props = inserter.getNodeProperties(id);
		if (props.containsKey(prop))
			return inserter.getNodeProperties(id).get(prop);
		return null;
	}
	
	public void setNodeProperty(Long id, String prop, Object val) {
		inserter.setNodeProperty(id, prop, val);
	}
	
	public Long createNonUniqueNonIndexedNode(NodeLabel label, Map<String, Object> properties) {
		Long id = inserter.createNode(properties, label);
		elementCount++;
		updateCount(label.toString());
        return id;
	}
	
	public Long createUniqueNonIndexedNode(NodeLabel label, Map<String, Object> properties, String lexiconLabel) {
		String lexiconIdLabel = label.toString();
		if (lexiconLabel != null && properties.containsKey(lexiconLabel))
			lexiconIdLabel = (String) properties.get(lexiconLabel);
		
		Long id = getNodeId(label, lexiconIdLabel, lexiconLabel, (String) properties.get(lexiconLabel), null, null, null);
		
		if (id == null) {
			id = inserter.createNode(properties, label);
			elementCount++;
			updateCount(label.toString());
        	addToLexicon(label.toString(), lexiconIdLabel, id, null, null, null);
		}
		
        return id;
	}
	
	public Long createNonUniqueIndexedNode(NodeLabel label, Map<String, Object> properties) {
		final BatchInserterIndex index = indexer.indexFor(label.toString());
        if (index == null)
            throw new IllegalStateException("Index '"+label.toString()+"' not configured.");
        
    	Long id = inserter.createNode(properties, label);
    	elementCount++;
		updateCount(label.toString());
    	index.add(id, properties);
        
        return id;
	}
	
	public Long createUniqueIndexedNode(NodeLabel label, Map<String, Object> properties, String lexiconLabel, String corpusLabel, String collectionTitle) {
		final BatchInserterIndex index = indexer.indexFor(label.toString());
        if (index == null)
            throw new IllegalStateException("Index '"+label.toString()+"' not configured.");

		String lexiconIdLabel = label.toString();
		if (lexiconLabel != null && properties.containsKey(lexiconLabel))
			lexiconIdLabel = (String) properties.get(lexiconLabel);
        
    	Long id = getNodeId(label, lexiconIdLabel, lexiconLabel, (String) properties.get(lexiconLabel), index, corpusLabel, collectionTitle);
        
        if (id == null) {
        	id = inserter.createNode(properties, label);
        	elementCount++;
    		updateCount(label.toString());
        	index.add(id, properties);
        	addToLexicon(label.toString(), lexiconIdLabel, id, index, corpusLabel, collectionTitle);
		}
        
        return id;
	}
	
	public void processLexicon() {
		monitor.log("Processing lexicon...");
		Iterator<String> labels = getCountKeys();
		while (labels.hasNext()) {
			String label = labels.next();
			monitor.log(""+label);
			if (lexicon.containsKey(label) && lexicon.get(label).size() > 0) {
				Iterator<String> nodes = lexicon.get(label).keySet().iterator();
				if (label.equals("WordType") || label.equals("Lemma") || label.equals("Phonetic") || label.equals("PosTag")) {
					monitor.log("Counting "+label);
					int i = 0;
					while (nodes.hasNext()) {
						i++;
						String n = nodes.next();
						inserter.setNodeProperty((Long) lexicon.get(label).get(n).get("id"), "token_count", lexicon.get(label).get(n).get("token_count"));
						if (corpusLexicon.containsKey(label)) {
							for (String corpus : corpusLexicon.get(label).keySet()) {
								if (corpusLexicon.get(label).get(corpus).containsKey(n))
									inserter.setNodeProperty((Long) lexicon.get(label).get(n).get("id"), "token_count_"+corpus, corpusLexicon.get(label).get(corpus).get(n));
								else
									inserter.setNodeProperty((Long) lexicon.get(label).get(n).get("id"), "token_count_"+corpus, 0);
							}
						}
						
//						if (collectionLexicon.containsKey(label)) {
//							for (String collection : collectionLexicon.get(label).keySet()) {
//								if (collectionLexicon.get(label).get(collection).containsKey(n))
//									inserter.setNodeProperty((Long) lexicon.get(label).get(n).get("id"), "token_count_"+collection, collectionLexicon.get(label).get(collection).get(n));
//								else
//									inserter.setNodeProperty((Long) lexicon.get(label).get(n).get("id"), "token_count_"+collection, 0);
//							}
//						}
					}
					monitor.log("Counted "+String.valueOf(i)+" nodes with label "+label);
				} else if (label.equals("PosHead") || label.equals("PosFeature")) {
					while (nodes.hasNext()) {
						String n = nodes.next();
						int t = 0;
						int p = 0;
						Iterator<String> posNodes = lexicon.get(NodeLabel.PosTag.toString()).keySet().iterator();
						while (posNodes.hasNext()) {
							String pos = posNodes.next();
							if ((label.equals("PosHead") && pos.startsWith(n)) || (label.equals("PosFeature") && pos.contains(n.split("=")[1]))) {
								t = t + (int) lexicon.get(NodeLabel.PosTag.toString()).get(pos).get("token_count");
								p++;
							}
						}
						inserter.setNodeProperty((Long) lexicon.get(label).get(n).get("id"), "pos_tag_count", p);
						inserter.setNodeProperty((Long) lexicon.get(label).get(n).get("id"), "token_count", t);
						if (corpusLexicon.containsKey(label)) {
							for (String corpus : corpusLexicon.get(label).keySet()) {
								if (corpusLexicon.get(label).get(corpus).containsKey(n))
									inserter.setNodeProperty((Long) lexicon.get(label).get(n).get("id"), "token_count_"+corpus, corpusLexicon.get(label).get(corpus).get(n));
								else
									inserter.setNodeProperty((Long) lexicon.get(label).get(n).get("id"), "token_count_"+corpus, 0);
							}
						}
					}
				}
//				else if (label.equals("Metadatum"))
//					while (nodes.hasNext()) {
//						String n = nodes.next();
//						inserter.setNodeProperty((Long) lexicon.get(label).get(n).get("id"), "document_count", lexicon.get(label).get(n).get("token_count"));
//					}
			} else {
				monitor.log("Nothing to process.");
			}
		}
		monitor.log("Finished processing lexicon.");
	}

}
