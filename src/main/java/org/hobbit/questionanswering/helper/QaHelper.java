package org.hobbit.questionanswering.helper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonArray;
import org.apache.jena.atlas.json.JsonObject;

import org.hobbit.QaldBuilder;

/**
 * A helper class used by QA benchmarking system
 * @author Mohammed Abdelgadir
 * @version 3.0.5
 */
public class QaHelper {
	
	private Random rand;
	private ArrayList<Integer> used;
	private ArrayList<String> data;
	QaldBuilder qald;
	HashMap<String, Integer> languages;
	
	private int numOfQuestions=0;
	private String sparqlService="http://dbpedia.org/sparql";
	
	/**
	 * The class constructor 
	 * @param seed : for randomisation
	 * @param numOfQuestions : number of questions to retrieve
	 * @param sparqlService : a url for sparql service
	 */
	public QaHelper(long seed,int numOfQuestions,String sparqlService) {
		this.numOfQuestions=numOfQuestions;
		this.sparqlService = sparqlService;
		
		this.languages=new HashMap<String, Integer>();
		this.data=new ArrayList<String>();
		this.used=new ArrayList<Integer>();
		this.rand=new Random(seed);
	}
	
	/**
	 * A function retrieves a list of Qald formated questions for large scale benchmarking
	 * @param fileName : The file name for data set (json)
	 * @return A list of Qald formated questions
	 * @throws Exception
	 */
	public  ArrayList<String> getLargeScaleData(String fileName) throws Exception {
		JsonArray questionsArray=JSON.readAny("data/"+fileName).getAsArray();
		int randNum=-1,datasetSize=questionsArray.size();
		while(data.size()<this.numOfQuestions) {
			randNum=rand.nextInt(datasetSize);
			if(!used.contains(randNum)) {
				qald = new QaldBuilder();
				qald.setQuestionAsJson(questionsArray.get(randNum).getAsObject().toString());
				qald.setAnswers(sparqlService);
				data.add(qald.getQaldQuestion());
				used.add(randNum);
			}
		}
		return this.data;
	}
	
	/**
	 * A function retrieves a list of Qald formated questions for multilingual benchmarking
	 * @param fileName : The file name for data set (json)
	 * @param lang : the questions language
	 * @return A list of Qald formated questions
	 * @throws Exception
	 */
	public ArrayList<String> getMultilingualData(String fileName,String lang) throws Exception {
		JsonArray questionsArray=JSON.readAny("data/"+fileName).getAsArray();
		this.setLanguages(questionsArray.get(0).getAsObject().get("question").getAsArray());
		int langID=this.languages.get(lang);
		JsonObject randomlySelected;
		int randNum=-1,datasetSize=questionsArray.size(),id=-1;
		String query="",question="";
		while(data.size()<this.numOfQuestions) {
			randNum=rand.nextInt(datasetSize);
			if(!used.contains(randNum)) {
				qald = new QaldBuilder();
				randomlySelected=questionsArray.get(randNum).getAsObject();
				question = randomlySelected.get("question").getAsArray().get(langID).getAsObject().get("string").toString().replace("\"", "").trim();
				query = randomlySelected.get("query").getAsArray().get(langID).getAsObject().get("sparql").toString().replace("\"", "").trim();
				id= Integer.parseInt(randomlySelected.get("id").toString().replace("\"", "").trim());
				qald.setID(id);
				qald.setAnswers(randomlySelected);
				qald.setQuestionString(question, lang);
				qald.setQuery(query);
				qald.setAnswers(sparqlService);
		
				data.add(qald.getQaldQuestion());
				used.add(randNum);
			}
		}
		return this.data;
	}
	
	/*
	 * Auxiliary function used by getMultilingualData function
	 */
	private void setLanguages(JsonArray languages) {
		String lang ="";
		for(int i =0;i<languages.size();i++) {
			lang = languages.get(i).getAsObject().get("language").toString().trim().replace("\"", "");
			this.languages.put(lang, i);
		}
    }
}