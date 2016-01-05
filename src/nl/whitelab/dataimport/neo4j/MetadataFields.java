package nl.whitelab.dataimport.neo4j;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

public class MetadataFields {
    private JSONObject metadataFields = null;
    private JSONObject actorFields = null;
    private Map<String,Map<String,Object>> groupProperties;
    private Map<String,Map<String,Object>> actorProperties;

	public MetadataFields(String file) {
		try {
			metadataFields = new JSONObject(readFile(file, StandardCharsets.UTF_8));
			groupProperties = new HashMap<String,Map<String,Object>>();
			JSONObject fieldInfo = metadataFields.getJSONObject("fieldInfo");
			metadataFields = (JSONObject) fieldInfo.getJSONObject("metadataFields");
			Iterator<String> groups = (Iterator<String>) this.metadataFields.keys();
			while (groups.hasNext()) {
				String group = groups.next();
//				System.out.println("Loading metadata field '"+group+"'...");
				Map<String,Object> groupProps = this.loadMetadatumGroupProperties(group);
				groupProperties.put(group, groupProps);
			}
			if (fieldInfo.has("actorFields")) {
				actorProperties = new HashMap<String,Map<String,Object>>();
				actorFields = (JSONObject) fieldInfo.getJSONObject("actorFields");
				Iterator<String> actorGroups = (Iterator<String>) this.actorFields.keys();
				while (actorGroups.hasNext()) {
					String group = actorGroups.next();
//					System.out.println("Loading actor field '"+group+"'...");
					Map<String,Object> actorGroupProps = this.loadActorGroupProperties(group);
					actorProperties.put(group, actorGroupProps);
				}
			}
		} catch (JSONException | IOException e) {
			e.printStackTrace();
		}
	}

    private Map<String,Object> loadMetadatumGroupProperties(String element) {
    	Map<String,Object> properties = new HashMap<String, Object>();
    	properties.put("label", element);
    	
    	try {
			JSONObject group = (JSONObject) metadataFields.getJSONObject(element);
			properties = processGroupKeys(group, properties);
		} catch (JSONException e) {
			e.printStackTrace();
		}
    	
		return properties;
	}

    private Map<String,Object> loadActorGroupProperties(String element) {
    	Map<String,Object> properties = new HashMap<String, Object>();
    	properties.put("label", element);
    	
    	try {
			JSONObject group = (JSONObject) actorFields.getJSONObject(element);
			properties = processGroupKeys(group, properties);
		} catch (JSONException e) {
			e.printStackTrace();
		}
    	
		return properties;
	}
    
    private Map<String,Object> processGroupKeys(JSONObject group, Map<String,Object> properties) throws JSONException {
		Iterator<String> keys = (Iterator<String>) group.keys();
		while (keys.hasNext()) {
			String key = keys.next();
			if (key.equals("valueCount"))
				properties.put("values", (int) group.getInt(key));
			else if (key.equals("values") && !properties.containsKey("values"))
				properties.put(key, ((JSONObject) group.getJSONObject(key)).length());
//			else if (key.equals("valueListComplete"))
//				properties.put(camelToUnderscore(key), group.getBoolean(key));
			else if (!key.equals("valueListComplete"))
				properties.put(camelToUnderscore(key), group.getString(key));
		}
    	return properties;
    }
    
    private String camelToUnderscore(String str) {
        String regex = "([a-z])([A-Z])";
        String replacement = "$1_$2";
        return str.replaceAll(regex, replacement).toLowerCase();
    }
    
    public Map<String,Object> getActorGroupProperties(String group) {
    	if (actorProperties.containsKey(group))
    		return actorProperties.get(group);
    	return null;
    }
    
    public Map<String,Object> getMetadatumGroupProperties(String group) {
    	if (groupProperties.containsKey(group))
    		return groupProperties.get(group);
    	return null;
    }
	
	private static String readFile(String path, Charset encoding) throws IOException {
	  byte[] encoded = Files.readAllBytes(Paths.get(path));
	  return new String(encoded, encoding);
	}

}
