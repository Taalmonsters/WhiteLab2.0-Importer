package nl.whitelab.dataimport.neo4j;

import org.neo4j.graphdb.RelationshipType;

public enum LinkLabel implements RelationshipType { HAS_COLLECTION, HAS_DOCUMENT, HAS_KEY, HAS_METADATUM, HAS_TOKEN, HAS_TYPE, HAS_LEMMA, HAS_PHONETIC, HAS_POS_TAG, HAS_HEAD, HAS_FEATURE, NEXT, STARTS_AT, ENDS_AT, OCCURS_IN }
