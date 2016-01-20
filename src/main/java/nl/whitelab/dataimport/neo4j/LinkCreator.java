package nl.whitelab.dataimport.neo4j;

import java.util.Map;

import nl.whitelab.dataimport.tools.IndexTool;
import nl.whitelab.dataimport.util.ProgressMonitor;

import org.neo4j.unsafe.batchinsert.BatchInserterIndex;

public class LinkCreator extends ElementCreator {

	public LinkCreator(IndexTool in, ProgressMonitor m) {
		super(in, m);
	}
	
	public Long getLinkId(LinkLabel label, String lexiconIdLabel, String propertyLabel, String propertyValue, BatchInserterIndex index, String corpusLabel, String collectionTitle) {
		return getElementId(label.toString(), lexiconIdLabel, propertyLabel, propertyValue, index, corpusLabel, collectionTitle);
	}
	
	public int getLinkCount() {
		return getElementCount();
	}
	
	public long getLinkCount(String label) {
		return getElementCount(label);
	}
	
	public Long createNonUniqueNonIndexedLink(long nodeId1, long nodeId2, LinkLabel label, Map<String, Object> properties) {
		Long id = inserter.createRelationship(nodeId1, nodeId2, label, properties);
		elementCount++;
		updateCount(label.toString());
        return id;
	}
	
	public Long createUniqueNonIndexedLink(long nodeId1, long nodeId2, LinkLabel label, Map<String, Object> properties, String lexiconLabel) {
		String lexiconIdLabel = String.valueOf(nodeId1)+":"+String.valueOf(nodeId2);
        if (lexiconLabel != null)
        	lexiconIdLabel = String.valueOf(nodeId1)+":"+String.valueOf(properties.get(lexiconLabel))+":"+String.valueOf(nodeId2);
		
        Long id = getLinkId(label, lexiconIdLabel, lexiconLabel, String.valueOf(properties.get(lexiconLabel)), null, null, null);
		
		if (id == null) {
			id = inserter.createRelationship(nodeId1, nodeId2, label, properties);
			elementCount++;
			updateCount(label.toString());
        	addToLexicon(label.toString(), lexiconIdLabel, id, null, null, null);
		}
		
        return id;
	}
	
	public Long createUniqueIndexedLink(long nodeId1, long nodeId2, LinkLabel label, Map<String, Object> properties, String lexiconLabel) {
		final BatchInserterIndex index = indexer.indexFor(label.toString());
        if (index == null)
            throw new IllegalStateException("Index '"+label.toString()+"' not configured.");
        
        String lexiconIdLabel = String.valueOf(nodeId1)+":"+String.valueOf(nodeId2);
        if (lexiconLabel != null)
        	lexiconIdLabel = String.valueOf(nodeId1)+":"+String.valueOf(properties.get(lexiconLabel))+":"+String.valueOf(nodeId2);
        
    	Long id = getLinkId(label, lexiconIdLabel, lexiconLabel, String.valueOf(properties.get(lexiconLabel)), index, null, null);
        
        if (id == null) {
        	id = inserter.createRelationship(nodeId1, nodeId2, label, properties);
        	elementCount++;
    		updateCount(label.toString());
    		properties.put("label", lexiconIdLabel);
        	index.add(id, properties);
        	addToLexicon(label.toString(), lexiconIdLabel, id, index, null, null);
		}
        
        return id;
	}
	
	@Override
	protected Long getElementId(String label, String lexiconIdLabel, String propertyLabel, String propertyValue, BatchInserterIndex index, String corpusLabel, String collectionTitle) {
		Long id = checkLexicon(label, lexiconIdLabel, corpusLabel, collectionTitle);
        if (id == null && index != null)
        	id = index.get("label", lexiconIdLabel).getSingle();
        
        return id;
	}

}
