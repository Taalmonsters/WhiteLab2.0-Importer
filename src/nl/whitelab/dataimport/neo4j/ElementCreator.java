package nl.whitelab.dataimport.neo4j;

import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import nl.whitelab.dataimport.tools.IndexTool;
import nl.whitelab.dataimport.util.ProgressMonitor;

import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;

public abstract class ElementCreator {
	protected static final int FLUSH_INTERVAL = 1000000;
	protected final IndexTool indexer;
	protected final BatchInserter inserter;
	protected final ProgressMonitor monitor;
	protected int elementCount = 0;
	protected Map<String,Long> labelCounts = new HashMap<String,Long>();
	protected Map<String,Object2ObjectOpenHashMap<String,Map<String,Object>>> lexicon = new HashMap<String,Object2ObjectOpenHashMap<String,Map<String,Object>>>();
	protected Map<String,Map<String,Object2ObjectOpenHashMap<String,Long>>> corpusLexicon = new HashMap<String,Map<String,Object2ObjectOpenHashMap<String,Long>>>();
//	protected Map<String,Map<String,Object2ObjectOpenHashMap<String,Long>>> collectionLexicon = new HashMap<String,Map<String,Object2ObjectOpenHashMap<String,Long>>>();

	public ElementCreator(IndexTool in, ProgressMonitor m) {
		indexer = in;
		inserter = indexer.getInserter();
		monitor = m;
	}

	protected static boolean isInteger(String s) {
		boolean isValidInteger = false;
		try {
			Integer.parseInt(s);
			isValidInteger = true;
		} catch (NumberFormatException ex) {
		}
		return isValidInteger;
	}
	
	protected int getElementCount() {
		return elementCount;
	}
	
	public long getElementCount(String label) {
		if (labelCounts.containsKey(label))
			return labelCounts.get(label);
		
		return 0;
	}
	
	protected void updateCount(String label) {
		if (!labelCounts.containsKey(label))
			labelCounts.put(label, (long) 0);
		
		labelCounts.put(label, labelCounts.get(label) + 1);
	}
	
	public Iterator<String> getCountKeys() {
		return labelCounts.keySet().iterator();
	}
	
	protected boolean timeToFlush(String label) {
		int interval = indexer.getFlushInterval(label);
		if ((interval > -1 && lexicon.get(label).size() % interval == 0) || (interval == -1 && lexicon.get(label).size() % FLUSH_INTERVAL == 0))
			return true;
		return false;
	}
	
	protected void flushIndex(BatchInserterIndex index, String label) {
		System.err.println("Flushing "+label);
		index.flush();
	}
	
	protected Long getElementId(String label, String lexiconIdLabel, String propertyLabel, String propertyValue, BatchInserterIndex index, String corpusLabel, String collectionTitle) {
		Long id = checkLexicon(label, lexiconIdLabel,corpusLabel,collectionTitle);
        if (id == null && index != null)
        	id = index.get(propertyLabel, propertyValue).getSingle();
        
        return id;
	}

	protected Long checkLexicon(String label, String elementName, String corpusLabel, String collectionTitle) {
		if (lexicon.containsKey(label) && lexicon.get(label).containsKey(elementName)) {
			lexicon.get(label).get(elementName).put("token_count", (int) lexicon.get(label).get(elementName).get("token_count") + 1);
			if (corpusLabel != null) {
				updateCorpusCount(label, elementName, corpusLabel);
			}
			
//			if (collectionTitle != null) {
//				updateCollectionCount(label, elementName, collectionTitle);
//			}
			
			return (Long) lexicon.get(label).get(elementName).get("id");
		}
		return null;
	}
	
	protected void updateCorpusCount(String label, String elementName, String corpusLabel) {
		if (!corpusLexicon.containsKey(label))
			corpusLexicon.put(label, new HashMap<String,Object2ObjectOpenHashMap<String,Long>>());
		if (!corpusLexicon.get(label).containsKey(corpusLabel))
			corpusLexicon.get(label).put(corpusLabel, new Object2ObjectOpenHashMap<String,Long>());
		if (!corpusLexicon.get(label).get(corpusLabel).containsKey(elementName))
			corpusLexicon.get(label).get(corpusLabel).put(elementName, (long) 0);
		corpusLexicon.get(label).get(corpusLabel).put(elementName, corpusLexicon.get(label).get(corpusLabel).get(elementName) + 1);
	}
	
//	protected void updateCollectionCount(String label, String elementName, String collectionTitle) {
//		if (!corpusLexicon.containsKey(label))
//			corpusLexicon.put(label, new HashMap<String,Object2ObjectOpenHashMap<String,Long>>());
//		if (!corpusLexicon.get(label).containsKey(collectionTitle))
//			corpusLexicon.get(label).put(collectionTitle, new Object2ObjectOpenHashMap<String,Long>());
//		if (!corpusLexicon.get(label).get(collectionTitle).containsKey(elementName))
//			corpusLexicon.get(label).get(collectionTitle).put(elementName, (long) 0);
//		corpusLexicon.get(label).get(collectionTitle).put(elementName, corpusLexicon.get(label).get(collectionTitle).get(elementName) + 1);
//	}
	
	protected void addToLexicon(String label, String elementName, Long elementId, BatchInserterIndex index, String corpusLabel, String collectionTitle) {
		if (!lexicon.containsKey(label))
			lexicon.put(label, new Object2ObjectOpenHashMap<String,Map<String,Object>>());
		
		if (!lexicon.get(label).containsKey(elementName)) {
			Map<String,Object> stuff = new HashMap<String,Object>();
			stuff.put("token_count", 1);
			stuff.put("id", elementId);
			lexicon.get(label).put(elementName, stuff);
		} else {
			Map<String,Object> stuff = lexicon.get(label).get(elementName);
			stuff.put("token_count", (Long) stuff.get("token_count") + 1);
		}
		
		if (corpusLabel != null) {
			updateCorpusCount(label, elementName, corpusLabel);
		}
		
//		if (collectionTitle != null) {
//			updateCollectionCount(label, elementName, collectionTitle);
//		}
		
		if (index != null && timeToFlush(label))
			flushIndex(index, label);
	}

}
