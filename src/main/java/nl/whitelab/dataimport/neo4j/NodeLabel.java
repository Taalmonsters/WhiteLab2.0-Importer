package nl.whitelab.dataimport.neo4j;

import org.neo4j.graphdb.Label;

public enum NodeLabel implements Label { NodeCounter, Corpus, Collection, Document, Metadatum, WordToken, WordType, Lemma, PosTag, PosHead, PosFeature, Phonetic, Sentence, ParagraphStart }
