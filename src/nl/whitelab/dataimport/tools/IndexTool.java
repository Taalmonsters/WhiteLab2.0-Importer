package nl.whitelab.dataimport.tools;

import gnu.trove.map.hash.THashMap;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import com.google.common.base.CaseFormat;

import nl.whitelab.dataimport.neo4j.Corpus;
import nl.whitelab.dataimport.neo4j.LinkLabel;
import nl.whitelab.dataimport.neo4j.NodeCreator;
import nl.whitelab.dataimport.neo4j.LinkCreator;
import nl.whitelab.dataimport.neo4j.NodeLabel;
import nl.whitelab.dataimport.util.ProgressMonitor;

public class IndexTool {
    private static final Map<String, String> FULLTEXT_CONFIG = Collections.singletonMap(IndexManager.PROVIDER,"fulltext");
    private static final Map<String, String> EXACT_CONFIG = Collections.singletonMap(IndexManager.PROVIDER,"exact");
    private static final Map<String, String> SPATIAL_CONFIG = Collections.singletonMap(IndexManager.PROVIDER,"spatial");
    private final String graphDir;
    private final String configFile;
    private final Map<String,String> config;
    private final BatchInserter inserter;
    private final BatchInserterIndexProvider indexProvider;
    private Map<String,BatchInserterIndex> indexes = new HashMap<String, BatchInserterIndex>();
    private Map<String,Integer> flushIntervals = new HashMap<String, Integer>();
    private final ProgressMonitor monitor;
	private static File inputDir;
	private final THashMap<String,Corpus> corpora;
    private static NodeCreator nodeCreator;
    private static LinkCreator linkCreator;
    public boolean doIndexing = false;
    public boolean recreateMetadataIndex = false;
    public boolean countTokens = false;

	public static void main(String[] args) throws IOException, ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		if (args.length < 3) {
			System.err.println("Usage: java -jar WhiteLab-Importer.jar /path/to/corpus/parent/dir /path/to/neo4j/db [create|append] [options]");
			System.err.println("");
			System.err.println("Options:");
			System.err.println("-config /path/to/config/batch.properties: path to batch inserter configuration file (default: batch.properties)");
			System.err.println("-r [true|false]: whether or not to recreate metadata index files before importing (default: false)");
			System.err.println("-c [true|false]: whether or not to add token counts after indexing (default: false)");
			System.err.println("-p [0-9+]: progress report interval in minutes (default: 10)");
			System.err.println("");
		} else {

	        if (new File(args[1]).exists() && existsInArgs("create", args)) {
	            FileUtils.deleteRecursively(new File(args[1]));
	        }
	        
			IndexTool indexer = new IndexTool(args);
			indexer.processCorpora();
		}
	}
	
	public static boolean existsInArgs(String label, String[] args) {
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals(label))
				return true;
		}
		return false;
	}
	
	public static String getValueFromArgs(String label, String[] args, String def) {
		for (int i = 0; i < args.length; i++) {
			if (args[i].equals(label))
				return args[i+1];
		}
		return def;
	}

	public IndexTool(String[] args) throws IOException {
		if (args.length > 2) {
	        for (int i = 2; i < args.length; i++) {
				if (args[i].equals("-i"))
					doIndexing = Boolean.parseBoolean(args[i+1]);
				else if (args[i].equals("-r"))
					recreateMetadataIndex = Boolean.parseBoolean(args[i+1]);
				else if (args[i].equals("-c"))
					countTokens = Boolean.parseBoolean(args[i+1]);
			}
		}
		
		graphDir = args[1];
		configFile = getValueFromArgs("config", args, "batch.properties");
		config = MapUtil.load(new File(configFile));
		inserter = createBatchInserter(new File(graphDir), config);

        indexProvider = createIndexProvider();
        initializeIndexes();
		setConstraints();
        
		inputDir = new File(args[0]);
		if (!inputDir.isDirectory() || !inputDir.canRead())
			inputDir = null;
		
		
		monitor = new ProgressMonitor(Integer.parseInt(getValueFromArgs("-p",args,"10")));
		
		corpora = new THashMap<String,Corpus>();
		
		nodeCreator = new NodeCreator(this, monitor);
		linkCreator = new LinkCreator(this, monitor);
	}

    protected BatchInserterIndexProvider createIndexProvider() {
    	return new LuceneBatchInserterIndexProvider( inserter );
    }

    @SuppressWarnings("deprecation")
    protected BatchInserter createBatchInserter(File graphDb, Map<String, String> c) {
        return BatchInserters.inserter(graphDb.getAbsolutePath(), c);
    }
    
    private void initializeIndexes() {
    	initializeNodeIndex(NodeLabel.Corpus.toString(), "fulltext", "label", 5);
    	initializeNodeIndex(NodeLabel.Collection.toString(), "fulltext", "title", 10);
    	initializeNodeIndex(NodeLabel.Document.toString(), "fulltext", "xmlid", 500000);
    	initializeNodeIndex(NodeLabel.Metadatum.toString(), "fulltext", "label", 100);
    	initializeNodeIndex(NodeLabel.WordType.toString(), "fulltext", "label", 1000000);
    	initializeNodeIndex(NodeLabel.Lemma.toString(), "fulltext", "label", 1000000);
    	initializeNodeIndex(NodeLabel.Phonetic.toString(), "fulltext", "label", 50000);
    	initializeNodeIndex(NodeLabel.PosTag.toString(), "fulltext", "label", 300);
    	initializeNodeIndex(NodeLabel.PosHead.toString(), "fulltext", "label", 15);
    	initializeNodeIndex(NodeLabel.PosFeature.toString(), "fulltext", "label", 100);
    	initializeRelationshipIndex(LinkLabel.HAS_METADATUM.toString(), "fulltext", "value", 500000);
    }
    
    private void initializeNodeIndex(String label, String indexType, String indexPropertyLabel, int cacheSize) {
    	BatchInserterIndex index = indexProvider.nodeIndex( label, MapUtil.stringMap( "type", indexType, "to_lower_case", "true" ) );
    	index.setCacheCapacity( indexPropertyLabel, cacheSize );
    	indexes.put(label, index);
    	flushIntervals.put(label, cacheSize);
    }
    
    private void initializeRelationshipIndex(String label, String indexType, String indexPropertyLabel, int cacheSize) {
    	BatchInserterIndex index = indexProvider.relationshipIndex( label, MapUtil.stringMap( "type", indexType, "to_lower_case", "true" ) );
    	index.setCacheCapacity( indexPropertyLabel, cacheSize );
    	indexes.put(label, index);
    }
    
    private void closeIndexes() {
    	Iterator<String> it = indexes.keySet().iterator();
    	while (it.hasNext()) {
    		String label = it.next();
    		BatchInserterIndex index = indexes.get(label);
    		index.flush();
    	}
    }

    public BatchInserterIndex indexFor(String index) {
        return indexes.get(index);
    }

    public BatchInserterIndex nodeIndexFor(String indexName, String indexType) {
        return indexProvider.nodeIndex(indexName, configFor(indexType));
    }

    public BatchInserterIndex relationshipIndexFor(String indexName, String indexType) {
        return indexProvider.relationshipIndex(indexName, configFor(indexType));
    }

    private Map<String, String> configFor(String indexType) {
        if (indexType.equalsIgnoreCase("fulltext")) return FULLTEXT_CONFIG;
        if (indexType.equalsIgnoreCase("spatial")) return SPATIAL_CONFIG;
        return EXACT_CONFIG;
    }
	
	public void setConstraints() {
		inserter.createDeferredConstraint(NodeLabel.Corpus).assertPropertyIsUnique("label").create();
        inserter.createDeferredConstraint(NodeLabel.Collection).assertPropertyIsUnique("title").create();
        inserter.createDeferredConstraint(NodeLabel.Document).assertPropertyIsUnique("xmlid").create();
        inserter.createDeferredConstraint(NodeLabel.Metadatum).assertPropertyIsUnique("label").create();
		inserter.createDeferredConstraint(NodeLabel.WordType).assertPropertyIsUnique("label").create();
        inserter.createDeferredConstraint(NodeLabel.Lemma).assertPropertyIsUnique("label").create();
        inserter.createDeferredConstraint(NodeLabel.Phonetic).assertPropertyIsUnique("label").create();
        inserter.createDeferredConstraint(NodeLabel.PosTag).assertPropertyIsUnique("label").create();
        inserter.createDeferredConstraint(NodeLabel.PosHead).assertPropertyIsUnique("label").create();
        inserter.createDeferredConstraint(NodeLabel.PosFeature).assertPropertyIsUnique("label").create();
	}
	
	public Map<String,String> getConfig() {
		return config;
	}
	
	public File getInputDir() {
		return inputDir;
	}
	
	public void processCorpora() throws ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
		monitor.start();
		String[] contents = inputDir.list();
		monitor.log("Input directory: "+inputDir.getAbsolutePath());
		List<String> corpusDirs = new ArrayList<String>();
		
		for (String file : contents) {
			corpusDirs.add(file);
		}
		
		monitor.log("Found "+String.valueOf(corpusDirs.size())+" corpora ('"+StringUtils.join(corpusDirs.toArray(), "','")+"').");
		
		for (String file : corpusDirs) {
			File corpusDir = new File(inputDir.getAbsolutePath()+"/"+file);
		    if (corpusDir.isDirectory()) {
				monitor.log("Corpus directory: "+file);
				Corpus corpus = new Corpus(file, corpusDir.getAbsolutePath(), nodeCreator, linkCreator, monitor);
		        corpora.put(corpus.getTitle(),corpus);
		        
		        if (recreateMetadataIndex) {
		        	monitor.log("Recreating metadata index for corpus '"+corpus.getTitle()+"'...");
		        	MetadataExtractor extractor = new MetadataExtractor(corpus.metadataZipFile, corpus.metadataFormat, corpus.getTitle());
		        	extractor.extract();
		        	monitor.log("Finished recreating metadata index for corpus '"+corpus.getTitle()+"'.");
		        }
		        	
		        try {
					corpus.process();
				} catch (IOException e) {
					System.err.println("Failed to process corpus: "+corpus.getTitle());
					e.printStackTrace();
				}
		    }
		}
		
		if (countTokens) {
			createNodeCounterNode();
			nodeCreator.processLexicon();
		}
		monitor.stop();
		monitor.endReport(nodeCreator, linkCreator);
		monitor.log("Finished corpus indexing. Closing database...");
		closeIndexes();
		indexProvider.shutdown();
        inserter.shutdown();
		monitor.log("Done.");
	}
	
	private void createNodeCounterNode() {
		Iterator<String> labels = nodeCreator.getCountKeys();
		Map<String,Object> properties = new HashMap<String,Object>();
		while (labels.hasNext()) {
			String label = labels.next();
			String lowerLabel = CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, label.toString());
			properties.put(lowerLabel+"_count", nodeCreator.getNodeCount(label));
		}
		
		properties.put("status", "finished");
		nodeCreator.createNonUniqueNonIndexedNode(NodeLabel.NodeCounter, properties);
	}
	
	public BatchInserter getInserter() {
		return inserter;
	}

	public int getFlushInterval(String label) {
		if (flushIntervals.containsKey(label))
			return flushIntervals.get(label);
		return -1;
	}

}
