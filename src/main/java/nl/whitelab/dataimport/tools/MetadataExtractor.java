package nl.whitelab.dataimport.tools;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.Attribute;
import javax.xml.stream.events.StartElement;
import javax.xml.stream.events.XMLEvent;

import nl.whitelab.dataimport.util.HumanReadableFormatter;
import nl.whitelab.dataimport.util.ProgressMonitor;

import org.codehaus.stax2.XMLInputFactory2;
import org.json.JSONException;
import org.json.JSONObject;

public class MetadataExtractor {
    private static final XMLInputFactory XML_INPUT_FACTORY = XMLInputFactory2.newInstance();
    
	private final String corpus;
	private final String input;
	private final String output;
	private final String format;
    private final ProgressMonitor monitor;
	private Map<String,Map<String,Object>> fields = new HashMap<String,Map<String,Object>>();
	private Map<String,Map<String,Object>> actorFields = new HashMap<String,Map<String,Object>>();

	public MetadataExtractor(String file, String f, String c) {
		corpus = c;
		input = file;
		String[] parts = file.split("/");
		output = file.replace(parts[parts.length-1], "indexmetadata.json");
		format = f.toLowerCase();
		monitor = new ProgressMonitor();
	}

	public static void main(String[] args) {
		if (args.length < 3)
			System.err.println("Usage: java -cp \"WhiteLab-Importer.jar\" nl.whitelab.dataimport.tools.MetadataExtractor /path/to/corpus/metadata.zip format corpus");
		else {
			long startTime = System.currentTimeMillis();
			MetadataExtractor extractor = new MetadataExtractor(args[0], args[1], args[2]);
			extractor.extract();
			long endTime = System.currentTimeMillis();
			System.out.println("Done extracting metadata from corpus: "+args[2]+". Duration: "+HumanReadableFormatter.humanReadableTimeElapsed(endTime - startTime));
		}
	}
	
	public void extract() {
		monitor.start();
		String currentFile = "";
		
		try {
			FileInputStream fis = new FileInputStream(input);
	        @SuppressWarnings("resource")
			ZipInputStream zis = new ZipInputStream(fis);
	        ZipEntry entry;
	        while ((entry = zis.getNextEntry()) != null) {
	        	currentFile = entry.getName();
	        	if (!entry.isDirectory() && currentFile.endsWith(format)) {
					long tagCount = 0;
		            int size;
		            byte[] buffer = new byte[2048];
	
		            ByteArrayOutputStream out = new ByteArrayOutputStream();
	
		            while ((size = zis.read(buffer, 0, buffer.length)) != -1) {
		              out.write(buffer, 0, size);
		            }
		            out.flush();
	
		            InputStream in = new ByteArrayInputStream(out.toByteArray());
					if (format.equals("imdi"))
						tagCount = processImdi(in);
					else if (format.equals("cmdi"))
						tagCount = processCmdi(in);

		            out.close();
					monitor.update(entry.getSize(),1,tagCount);
	        	} else {
	        		monitor.log("Skipping: " + currentFile);
	        	}
	        }
		} catch (IOException | XMLStreamException | JSONException e2) {
			System.err.println("Failed to process file: " + currentFile);
			e2.printStackTrace();
		} finally {
			try {
				writeFieldsToFile();
			} catch (IOException | JSONException e) {
				e.printStackTrace();
			}
		}
		
		monitor.stop();
	}
	
	private Long processImdi(InputStream in) throws XMLStreamException, JSONException, IOException {
		long t = 0;
		
		XMLEventReader reader = XML_INPUT_FACTORY.createXMLEventReader(in);
        try {
    		LinkedList<String> tags = new LinkedList<String>();
    		String group = null;
    		boolean insideLang = false;
    		String langDescr = null;
    		String langId = null;
    		String langName = null;
    		List<String> skipTags = new ArrayList<String>(Arrays.asList("CommunicationContext", "Keys", "Description", "Languages"));
            StringBuilder textBuffer = new StringBuilder();
            StartElement start = null;

            while (reader.hasNext()) {
            	XMLEvent event = (XMLEvent) reader.next();
                if (event.isStartElement()) {
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
                	String parent = "";
                	if (tags.size() > 0)
                		parent = tags.getLast();
                	
                	if (parent != null && parent.equals("MDGroup")) {
                		group = tag;
                	} else if (group != null && group.equals("Actors") && tag.equals("Languages")) {
                		insideLang = true;
                	}
                	
                	tags.add(tag);
                } else if (event.isEndElement()) {
                    String tag = tags.pollLast();
                    String parent = null;
                    if (tags.size() > 0)
                    	parent = tags.pollLast();
                    
                    if (parent == null || parent.equals("MDGroup")) {
                		group = null;
                	} else if (group != null && parent.equals(group) && !group.equals("Actors") && !skipTags.contains(tag) && !group.equals("Project")) {
                    	if (textBuffer.length() > 0)
                    		addFieldValue(tag,textBuffer.toString().trim());
                    	else
                    		addFieldValue(tag,"");
                    	t++;
                    } else if (group != null && group.equals("Actors") && !skipTags.contains(tag)) {
                    	if (textBuffer.length() > 0)
                			addActorFieldValue(tag,textBuffer.toString().trim());
                    	else
                    		addActorFieldValue(tag,"");
                    	t++;
                	} else if (group != null && group.equals("Actors") && tag.equals("Language")) {
                		addActorFieldValue(langDescr+" Name",langName);
                		addActorFieldValue(langDescr+" ISO Code",langId);
                		langDescr = null;
                		langId = null;
                		langName = null;
                	} else if (group != null && group.equals("Actors") && tag.equals("Languages")) {
                		insideLang = false;
                	} else if (group != null && group.equals("Actors") && insideLang) {
                		if (tag.equals("Id"))
                			langId = textBuffer.toString().trim();
                		else if (tag.equals("Name"))
                			langName = textBuffer.toString().trim();
                		else if (tag.equals("Description"))
                			langDescr = textBuffer.toString().trim();
                	}
                    
                    textBuffer = new StringBuilder();
                    if (parent != null)
                    	tags.add(parent);
                } else if (event.isCharacters()) {
                	textBuffer.append(event.asCharacters().getData());
                }
            }
        } finally {
            reader.close();
            in.close();
        }
		
		return t;
	}

	private Long processCmdi(InputStream in) throws XMLStreamException {
		// TODO implement
		return (long) 0;
	}
	
	private void addFieldValue(String key, String value) {
		if (value.equals("Unspecified") || value.equals("") || value.equals("Unknown"))
			value = "unknown";
		if (!fields.containsKey(key)) {
			Map<String,Object> props = new HashMap<String,Object>();
			Map<String,Integer> values = new HashMap<String,Integer>();
			values.put(value,1);
			props.put("values", values);
			fields.put(key, props);
		} else {
			@SuppressWarnings("unchecked")
			Map<String,Integer> values = (Map<String,Integer>) fields.get(key).get("values");
			if (values.containsKey(value))
				values.put(value, values.get(value) + 1);
			else
				values.put(value, 1);
			fields.get(key).put("values", values);
		}
	}
	
	private void addActorFieldValue(String key, String value) {
		if (value.equals("Unspecified") || value.equals("") || value.equals("Unknown"))
			value = "unknown";
		if (!actorFields.containsKey(key)) {
			Map<String,Object> props = new HashMap<String,Object>();
			Map<String,Integer> values = new HashMap<String,Integer>();
			values.put(value,1);
			props.put("values", values);
			actorFields.put(key, props);
		} else {
			@SuppressWarnings("unchecked")
			Map<String,Integer> values = (Map<String,Integer>) actorFields.get(key).get("values");
			if (values.containsKey(value))
				values.put(value, values.get(value) + 1);
			else
				values.put(value, 1);
			actorFields.get(key).put("values", values);
		}
	}
	
	private JSONObject generateJSON() throws JSONException {
		JSONObject fieldData = fieldsToJSON(fields);
		JSONObject actorData = null;
		if (actorFields.size() > 0)
			actorData = fieldsToJSON(actorFields);
		return addFrame(fieldData, actorData);
	}
	
	private JSONObject fieldsToJSON(Map<String,Map<String,Object>> fieldData) throws JSONException {
		JSONObject data = new JSONObject();
		Iterator<String> it = (Iterator<String>) fieldData.keySet().iterator();
		while (it.hasNext()) {
			String field = it.next();
			JSONObject fieldJSON = new JSONObject();
			JSONObject valuesJSON = new JSONObject();
			@SuppressWarnings("unchecked")
			Map<String,Integer> values = (Map<String,Integer>) fieldData.get(field).get("values");
			Iterator<String> it2 = (Iterator<String>) values.keySet().iterator();
			int i = 0;
			while (it2.hasNext() && i < 50) {
				if (i < 50) {
					String value = it2.next();
					Integer count = values.get(value);
					valuesJSON.put(value, count);
				}
				i++;
			}
			fieldJSON.put("displayName", field);
			fieldJSON.put("type", "text");
			fieldJSON.put("valueCount", i);
			fieldJSON.put("values", valuesJSON);
			if (it2.hasNext())
				fieldJSON.put("valueListComplete", false);
			else
				fieldJSON.put("valueListComplete", true);
			fieldJSON.put("unknownValue", "unknown");
			if (values.containsKey("unknown") && values.size() > 1) {
				fieldJSON.put("unknownCondition", "MISSING_OR_EMPTY");
			} else if (values.containsKey("unknown") && values.size() == 1) {
				fieldJSON.put("unknownCondition", "ALWAYS");
			} else {
				fieldJSON.put("unknownCondition", "NEVER");
			}
			data.put(field.replaceAll(" ", ""), fieldJSON);
		}
		return data;
	}
	
	private JSONObject addFrame(JSONObject fieldData, JSONObject actorFieldData) throws JSONException {
		JSONObject data = new JSONObject();
		data.put("contentViewable", true);
		data.put("description", corpus);
		JSONObject fieldInfo = new JSONObject();
		fieldInfo.put("metadataFields", fieldData);
		if (actorFieldData != null)
			fieldInfo.put("actorFields", actorFieldData);
		data.put("fieldInfo", fieldInfo);
		return data;
	}
	
	private void writeFieldsToFile() throws JSONException, IOException {
		JSONObject data = generateJSON();
		FileWriter file = new FileWriter(output);
        try {
            file.write(data.toString(1));
        } catch (IOException e) {
            e.printStackTrace();
 
        } finally {
            file.flush();
            file.close();
        }
	}

}
