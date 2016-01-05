package nl.whitelab.dataimport.parsers;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;

import org.codehaus.stax2.XMLInputFactory2;

import nl.whitelab.dataimport.neo4j.LinkCreator;
import nl.whitelab.dataimport.neo4j.NodeCreator;
import nl.whitelab.dataimport.util.ProgressMonitor;

public abstract class ContentParser {
	protected static final XMLInputFactory XML_INPUT_FACTORY = XMLInputFactory2.newInstance();
	protected final String corpusLabel;
	protected String collectionTitle;

	protected static NodeCreator nodeCreator;
	protected static LinkCreator linkCreator;
	protected final ProgressMonitor monitor;

	public ContentParser(String c, NodeCreator nc, LinkCreator lc, ProgressMonitor m) {
		corpusLabel = c;
		nodeCreator = nc;
		linkCreator = lc;
		monitor = m;
	}
	
	public abstract Integer parse(Long documentId, String collection, InputStream is) throws XMLStreamException, IOException;

}
