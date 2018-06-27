package org.hobbit.questionanswering.helper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonArray;
import org.apache.jena.atlas.json.JsonObject;

public class QaHelper {
	
	private Random rand;
	private ArrayList<Integer> used;
	private ArrayList<String> data;
	QaldBuilder qald;
	HashMap<String, Integer> languages;
	
	private int numOfQuestions=0;
	private String sparqlService="http://dbpedia.org/sparql";
	
	public QaHelper(long seed,int numOfQuestions,String sparqlService) {
		this.numOfQuestions=numOfQuestions;
		this.sparqlService = sparqlService;
		
		this.languages=new HashMap<String, Integer>();
		this.data=new ArrayList<String>();
		this.used=new ArrayList<Integer>();
		this.rand=new Random(seed);
	}
	
	public  ArrayList<String> getLargeScaleData(String fileName) {
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
	
	public ArrayList<String> getMultilingualData(String fileName,String lang) {
		JsonArray questionsArray=JSON.readAny("data/"+fileName).getAsArray();
		this.setLanguages(questionsArray.get(0).getAsObject().get("question").getAsArray());
		int langID=this.languages.get(lang);
		JsonObject randomlySelected;
		int randNum=-1,datasetSize=questionsArray.size();
		String query="",question="";
		while(data.size()<this.numOfQuestions) {
			randNum=rand.nextInt(datasetSize);
			if(!used.contains(randNum)) {
				qald = new QaldBuilder();
				randomlySelected=questionsArray.get(randNum).getAsObject();
				question = randomlySelected.get("question").getAsArray().get(langID).getAsObject().get("string").toString().trim().replace("\"", "");
				query = randomlySelected.get("query").getAsArray().get(langID).getAsObject().get("sparql").toString();
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
	
	private void setLanguages(JsonArray languages) {
		String lang ="";
		for(int i =0;i<languages.size();i++) {
			lang = languages.get(i).getAsObject().get("language").toString().trim().replace("\"", "");
			this.languages.put(lang, i);
		}
    }
}