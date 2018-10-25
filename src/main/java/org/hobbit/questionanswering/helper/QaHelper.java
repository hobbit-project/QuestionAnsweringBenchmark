package org.hobbit.questionanswering.helper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonArray;
import org.apache.jena.atlas.json.JsonValue;
import org.hobbit.QaldBuilder;

/**
 * A helper class used by QA benchmarking system
 * @author Mohammed Abdelgadir
 * @version 3.0.5
 */
public class QaHelper {
	
	private List<JsonValue> data;
	QaldBuilder qald;
	private long seed;
	private int numOfQuestions;
	public static String SPARQL_SERVICE="http://dbpedia.org/sparql";
	private int langId;
	
	/**
	 * The class constructor 
	 * @param seed : for randomisation
	 * @param numOfQuestions : number of questions to retrieve
	 * @param sparqlService : a url for sparql service
	 */
	public QaHelper(long seed,int numOfQuestions) {
		this.numOfQuestions=numOfQuestions;
		this.data=new ArrayList<JsonValue>();
		this.seed = seed;
	}
	
	/**
	 * A function retrieves a list of Qald formated questions for large scale benchmarking
	 * @param fileName : The file name for data set (json)
	 * @return A list of Qald formated questions
	 * @throws Exception
	 */
	public List<JsonValue> getLargeScaleData(String fileName) throws Exception {
		JsonArray questionsArray=JSON.readAny(fileName).getAsArray();
		
		if(this.numOfQuestions>questionsArray.size())
			throw new Exception("Number of Quesrtions is bigger than the data set size!");
		
		Collections.shuffle(questionsArray, new Random(seed));
		
		return questionsArray.subList(0, numOfQuestions);
	}
	
	public List<JsonValue> getLargeScaleData(String fileName, int triple) throws Exception {
		
		JsonArray questionsArray=JSON.readAny(fileName).getAsArray();
		
		for(JsonValue obj:questionsArray) {
			qald = new QaldBuilder();
			qald.setQuestionAsJson(obj.toString());
			if(qald.getTriple()==triple) {
				this.data.add(obj);
			}
		}
		if(this.data.size()<numOfQuestions)
			throw new Exception("There is no enough questions has this triple!");
					
		Collections.shuffle(this.data, new Random(seed));
		return this.data.subList(0, numOfQuestions);
	}
	
	/**
	 * A function retrieves a list of Qald formated questions for multilingual benchmarking
	 * @param fileName : The file name for data set (json)
	 * @param lang : the questions language
	 * @return A list of Qald formated questions
	 * @throws Exception
	 */
	public List<JsonValue> getMultilingualData(String fileName,String lang) throws Exception {
		List<JsonValue> questionsArray=JSON.readAny(fileName).getAsArray();
		if(this.numOfQuestions>questionsArray.size())
			throw new Exception("Number of Quesrtions is bigger than the data set size!");
		Collections.shuffle(questionsArray, new Random(seed));
		questionsArray =  questionsArray.subList(0, numOfQuestions);
		this.setLanguages(questionsArray.get(0).getAsObject().get("question").getAsArray(),lang);
		for(JsonValue quest:questionsArray) {
			qald = new QaldBuilder();
			qald.setID(Integer.parseInt(quest.getAsObject().get("id").toString()));
			qald.setOnlydbo(Boolean.parseBoolean(quest.getAsObject().get("onlydbo").toString()));
			qald.setQuery(quest.getAsObject().get("query").getAsArray().get(this.langId).getAsObject().get("sparql").toString());
			qald.setQuestionString(quest.getAsObject().get("question").getAsArray().get(this.langId).getAsObject().get("string").toString(),
					quest.getAsObject().get("question").getAsArray().get(this.langId).getAsObject().get("language").toString());
			this.data.add(qald.getQaldQuestion());
		}
		return this.data;
	}
	
	/*
	 * Auxiliary function used by getMultilingualData function
	 */
	private void setLanguages(JsonArray languages,String lang) {
		String localLange = "";
		for(int i =0;i<languages.size();i++) {
			localLange = languages.get(i).getAsObject().get("language").toString().trim().replace("\"", "");
			if(lang.equalsIgnoreCase(localLange)) {
				this.langId = i;
				break;
			}
		}
    }
}