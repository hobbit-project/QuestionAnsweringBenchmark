package org.hobbit.questionanswering.helper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonArray;
import org.apache.jena.atlas.json.JsonValue;
import org.hobbit.QaldBuilder;
import org.hobbit.questionanswering.QaDataGenerator;

/**
 * The helper class used by QA benchmarking system to load data sets.
 * @author Mohammed Abdelgadir
 * @version 1.0.5
 */
public class QaHelper {
	private static final Logger LOGGER = LogManager.getLogger(QaDataGenerator.class);
	
	private List<JsonValue> data;
	QaldBuilder qald;
	private long seed;
	private int numOfQuestions;
	private String sparqlService;
	private int langId;
	private boolean getAnswers;
	
	
	/**
	 * The class constructor 
	 * @param seed : for randomization
	 * @param numOfQuestions : number of questions to retrieve
	 * @param sparqlService : a url for sparql service
	 */
	public QaHelper(long seed,int numOfQuestions,String sparqlService) {
		this.numOfQuestions=numOfQuestions;
		this.data=new ArrayList<JsonValue>();
		this.seed = seed;
		this.sparqlService = sparqlService;
		this.setGetAnswers(true);
	}
	
	/**
	 * To load large scale data set without considering Number of Triples.
	 * @param fileName : The file name of data set (json)
	 * @return A list of Qald formated questions as Json objects
	 * @throws Exception
	 */
	public List<JsonValue> getLargeScaleData(String fileName) throws Exception {
		JsonArray questionsArray=JSON.readAny(fileName).getAsArray();
		for(JsonValue quest:questionsArray) {
			qald = new QaldBuilder();
			qald.setQuestionAsJson(quest.toString());
			if(this.isGetAnswers()) {
				qald.setAnswers(this.sparqlService);
				if(this.qald.getAnswers().size()>0)
					this.data.add(qald.getQuestionAsQald());
			}else {
				qald.removeAnswers();
				this.data.add(qald.getQuestionAsQald());
			}
		}
		if(this.numOfQuestions>this.data.size())
			throw new Exception("Number of Quesrtions is bigger than the data set size!");
		
		Collections.shuffle(this.data, new Random(seed));
		return this.data.subList(0, numOfQuestions);
	}
	
	/**
	 * To load large scale data set with Number of Triples
	 * @param fileName: Data set file name (json)
	 * @param triple: To load questions have specific number of triples
	 * @return A list of Qald formated questions as Json objects
	 * @throws Exception
	 */
	public List<JsonValue> getLargeScaleData(String fileName, int triple) throws Exception {
		
		JsonArray questionsArray=JSON.readAny(fileName).getAsArray();
		
		for(JsonValue quest:questionsArray) {
			qald = new QaldBuilder();
			qald.setQuestionAsJson(quest.toString());
			if(qald.getTriple()==triple) {
				qald.removeTriple();
				if(this.isGetAnswers()) {
					this.qald.setAnswers(this.sparqlService);
					if(this.qald.getAnswers().size()>0)
						this.data.add(qald.getQuestionAsQald());
				}else {
					this.qald.removeAnswers();
					this.data.add(qald.getQuestionAsQald());
				}
			}
		}
		if(this.data.size()<numOfQuestions)
			throw new Exception("There is no enough questions has this triple!");
		LOGGER.info("QaHelper: "+this.data.size()+" questions has answers with triple "+triple);
		//System.out.println(this.data.size());
		Collections.shuffle(this.data, new Random(seed));
		return this.data.subList(0, numOfQuestions);
	}
	
	/**
	 * To load multilingual data set and filter questions by language.
	 * @param fileName : Data set file name (json)
	 * @param lang : the questions language
	 * @return A list of Qald formated questions as Json objects
	 * @throws Exception
	 */
	public List<JsonValue> getMultilingualData(String fileName,String lang) throws Exception {
		List<JsonValue> questionsArray=JSON.readAny(fileName).getAsArray();
		this.setLanguages(questionsArray.get(0).getAsObject().get("question").getAsArray(),lang);
		
		for(JsonValue quest:questionsArray) {
			qald = new QaldBuilder();
			qald.setID(Integer.parseInt(quest.getAsObject().get("id").toString()));
			qald.setOnlydbo(Boolean.parseBoolean(quest.getAsObject().get("onlydbo").toString()));
			qald.setQuery(quest.getAsObject().get("query").getAsArray().get(this.langId).getAsObject().get("sparql").toString());
			qald.setQuestionString(quest.getAsObject().get("question").getAsArray().get(this.langId).getAsObject().get("string").toString(),lang);
			if(this.isGetAnswers()) {
				qald.setAnswers(this.sparqlService);
				if(this.qald.getAnswers().size()>0)
					this.data.add(qald.getQuestionAsQald());
			}else
				this.data.add(qald.getQuestionAsQald());
		}
		
		if(this.numOfQuestions>data.size())
			throw new Exception("Number of Quesrtions is bigger than the data set size!");
		Collections.shuffle(data, new Random(seed));
		
		return this.data.subList(0, numOfQuestions);
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
	/** 
	 * @return the getAnswers
	 */
	public boolean isGetAnswers() {
		return getAnswers;
	}

	/** To set up functions to add answers to the data sets or not
	 *  - True if you want functions to add answers.
	 *  - False if you don't want functions to add answers.
	 * @param getAnswers: boolean argument
	 */
	public void setGetAnswers(boolean getAnswers) {
		this.getAnswers = getAnswers;
	}
}