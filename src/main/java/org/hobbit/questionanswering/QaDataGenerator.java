package org.hobbit.questionanswering;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.hobbit.core.components.AbstractDataGenerator;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.hobbit.questionanswering.helper.QaHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QaDataGenerator extends AbstractDataGenerator {
	private static final Logger LOGGER = LoggerFactory.getLogger(QaDataGenerator.class);
    
	public static final String EXPERIMENT_TYPE_PARAMETER_KEY = "qa.experiment_type";
	public static final String EXPERIMENT_TASK_PARAMETER_KEY = "qa.experiment_task";
    public static final String QUESTION_LANGUAGE_PARAMETER_KEY = "qa.question_language";
    public static final String NUMBER_OF_DOCUMENTS_PARAMETER_KEY = "qa.number_of_documents";
    public static final String SEED_PARAMETER_KEY = "qa.seed";
    public static final String SPARQL_SERVICE_PARAMETER_KEY = "qa.sparql_service";
    
    private String experimentTypeName;
	private String experimentTaskName;
    private String questionLanguage;
    private String sparqlService;
    private int numberOfDocuments;
    private long seed;

    private QaHelper qaHelper = new QaHelper();
    private ArrayList<ArrayList<ArrayList<String>>> templates = null;
    
    public void init() throws Exception {
    	LOGGER.info("Initializing.");
    	super.init();
        Map<String, String> env = System.getenv();
        
        //load experimentTypeName from environment
        if(env.containsKey(EXPERIMENT_TYPE_PARAMETER_KEY)) {
        	String value = env.get(EXPERIMENT_TYPE_PARAMETER_KEY);
            try {
            	experimentTypeName = value;
            	LOGGER.info("Got experiment type from the environment parameters: \""+experimentTypeName+"\"");
            } catch (Exception e) {
                LOGGER.error("Exception while trying to parse the experiment type. Aborting.", e);
                throw new Exception("Exception while trying to parse the experiment type. Aborting.", e);
            }
        } else {
            String msg = "Couldn't get \"" + EXPERIMENT_TYPE_PARAMETER_KEY + "\" from the properties. Aborting.";
            LOGGER.error(msg);
            throw new Exception(msg);
        }
        
        //load experimentTaskName from environment
        if(env.containsKey(EXPERIMENT_TASK_PARAMETER_KEY)) {
            String value = env.get(EXPERIMENT_TASK_PARAMETER_KEY);
            try {
            	experimentTaskName = value;
            	LOGGER.info("Got experiment task from the environment parameters: \""+experimentTaskName+"\"");
            } catch (Exception e) {
                LOGGER.error("Exception while trying to parse the experiment task. Aborting.", e);
                throw new Exception("Exception while trying to parse the experiment task. Aborting.", e);
            }
        } else {
            String msg = "Couldn't get \"" + EXPERIMENT_TASK_PARAMETER_KEY + "\" from the properties. Aborting.";
            LOGGER.error(msg);
            throw new Exception(msg);
        }
        
        //load question templates for chosen task type
        LOGGER.info("Loading question templates for "+experimentTaskName+".");
        try{
    		templates = qaHelper.getTemplates(experimentTaskName.toLowerCase());
    	}catch(Exception e){
    		String msg = "Exception while getting template data. Aborting.";
			LOGGER.error(msg, e);
			throw new Exception(msg, e);
    	}
        LOGGER.info("Question templates for "+experimentTaskName+" loaded.");
        
        //load questionLanguage from environment
        if(env.containsKey(QUESTION_LANGUAGE_PARAMETER_KEY)) {
            String value = env.get(QUESTION_LANGUAGE_PARAMETER_KEY);
            try {
            	questionLanguage = value;
            	LOGGER.info("Got language from the environment parameters: \""+questionLanguage+"\"");
            } catch (Exception e) {
                LOGGER.error("Exception while trying to parse the experiment language. Aborting.", e);
                throw new Exception("Exception while trying to parse the experiment language. Aborting.", e);
            }
        } else {
            String msg = "Couldn't get \"" + QUESTION_LANGUAGE_PARAMETER_KEY + "\" from the properties. Aborting.";
            LOGGER.error(msg);
            throw new Exception(msg);
        }
        
        //load number of documents from environment
        if(env.containsKey(NUMBER_OF_DOCUMENTS_PARAMETER_KEY)){
        	try {
                numberOfDocuments = Integer.parseInt(env.get(NUMBER_OF_DOCUMENTS_PARAMETER_KEY));
                LOGGER.info("Got number of documents from the environment parameters: \""+numberOfDocuments+"\"");
            } catch (NumberFormatException e) {
            	LOGGER.error("Exception while trying to parse the number of documents. Aborting.", e);
                throw new IllegalArgumentException("Exception while trying to parse the number of documents. Aborting.", e);
            }
        }else{
        	LOGGER.error("Couldn't get \"" + NUMBER_OF_DOCUMENTS_PARAMETER_KEY + "\" from the environment. Aborting.");
            throw new IllegalArgumentException("Couldn't get \"" + NUMBER_OF_DOCUMENTS_PARAMETER_KEY + "\" from the environment. Aborting.");
        }
        if(!experimentTaskName.equalsIgnoreCase("largescale")){
        	if(numberOfDocuments>templates.size()){
        		numberOfDocuments = templates.size();
        		LOGGER.error("Chosen number of documents is too high.");
        		LOGGER.info("Reducing number of documents to "+numberOfDocuments+".");
        	}
        }
        
        //load seed from environment
        if(env.containsKey(SEED_PARAMETER_KEY)){
        	try {
                seed = Long.parseLong(env.get(SEED_PARAMETER_KEY));
                LOGGER.info("Got seed from the environment parameters: \""+seed+"\"");
            } catch (NumberFormatException e) {
            	LOGGER.error("Exception while trying to parse the seed. Aborting.", e);
                throw new IllegalArgumentException("Exception while trying to parse the seed. Aborting.", e);
            }
        }else{
        	LOGGER.error("Couldn't get \"" + SEED_PARAMETER_KEY + "\" from the environment. Aborting.");
            throw new IllegalArgumentException("Couldn't get \"" + SEED_PARAMETER_KEY + "\" from the environment. Aborting.");
        }
        
        //load sparqlService from environment
        sparqlService = "";
        if(env.containsKey(SPARQL_SERVICE_PARAMETER_KEY)) {
            String value = env.get(SPARQL_SERVICE_PARAMETER_KEY);
            try {
            	sparqlService = value;
            	LOGGER.info("Got SPARQL service from the environment parameters: \""+sparqlService+"\"");
            } catch (Exception e) {
                LOGGER.error("Exception while trying to parse the SPARQL service. Aborting.", e);
                throw new Exception("Exception while trying to parse the SPARQL service. Aborting.", e);
            }
        } else {
            String msg = "Couldn't get \"" + SPARQL_SERVICE_PARAMETER_KEY + "\" from the properties. Aborting.";
            LOGGER.error(msg);
            throw new Exception(msg);
        }
        
        LOGGER.info("Initialized.");
    }

    public void generateData() throws Exception{
    	LOGGER.info("Generating data and sending it to the Task Generator.");
    	
    	int templatesSize = 0;
    	int pseudoRandomTemplate = 0;
    	int pseudoRandomQuestion = 0;
    	int numberOfVariousQuestions = 0;
    	int queryCounter = 0;
    	
    	String question = "";
		String query = "";
		String result = "";
		
		byte[] byteArrayDataGenerator2TaskGenerator;
    	
    	templatesSize = templates.size();
		pseudoRandomTemplate = (int) (seed%templatesSize);
    	
    	ArrayList<String> usedQuestions = new ArrayList<String>();
    	ArrayList<Integer> usedTemplates = new ArrayList<Integer>();
    	if(experimentTaskName.toLowerCase().equals("largescale")){
    		usedTemplates = new ArrayList<Integer>( Arrays.asList(
    				0,4,7,10,11,14,16,17,19,21,
    				24,27,34,38,46,48,49,50,53,57,
    				59,60,62,68,71,72,79,81,82,83,
    				89,90,95,100,101,105,112,114,115,116,
    				118,120,121,123,127,134,136,137,138,142));
    	}
    	ArrayList<Integer> depletedTemplates = new ArrayList<Integer>();

		boolean notEnoughDataGenerated = true;
		while(notEnoughDataGenerated){
			
			boolean depleted = false;
			boolean picking = true;
			while(picking){
				depleted = false;
				
				if(usedTemplates.contains(pseudoRandomTemplate)){
	    			while(usedTemplates.contains(pseudoRandomTemplate)){
	    				pseudoRandomTemplate = (pseudoRandomTemplate+1)%templatesSize;
	    			}
	    		}
				usedTemplates.add(pseudoRandomTemplate);
				
	    		numberOfVariousQuestions = templates.get(pseudoRandomTemplate).size();
	    		
	    		if(seed%numberOfVariousQuestions < 0.2*numberOfVariousQuestions){
	    			pseudoRandomQuestion = ((int) Math.round((seed)+(0.2*numberOfVariousQuestions)))%numberOfVariousQuestions;
	    		}else{
	    			pseudoRandomQuestion = (int) ((seed)%numberOfVariousQuestions);
	    		}
	    		
	    		int pickingCounter = 0;
	    		if(usedQuestions.contains(""+pseudoRandomTemplate+","+pseudoRandomQuestion)){
	    			while(usedQuestions.contains(""+pseudoRandomTemplate+","+pseudoRandomQuestion)){
	    				pseudoRandomQuestion = (pseudoRandomQuestion+((int)Math.round((0.2*numberOfVariousQuestions))+1))%numberOfVariousQuestions;
	    				pickingCounter ++;
	    				if(depleted) break;
	    				if(pickingCounter>=numberOfVariousQuestions){
	    					depletedTemplates.add(pseudoRandomTemplate);
	    					depleted = true;
	    				}
	    			}
	    		}
	    		if(!depleted){
	    			usedQuestions.add(""+pseudoRandomTemplate+","+pseudoRandomQuestion);
	    			picking = false;
	    		}
			}
    		
    		String questionWord = "";
    		question = templates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(1);
        	questionWord = question.split(" ")[0].toLowerCase();
        	query = templates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(2);
			
        	String answertype, aggregation, onlydbo, hybrid, answerhead, keywords, hybridResult;
        	answertype = aggregation = onlydbo = hybrid = answerhead = keywords = hybridResult = "metainfo.missing";
        	
        	if(!experimentTaskName.toLowerCase().equals("largescale")){
	        	answertype = templates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(3);
	            aggregation = templates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(4);
	            onlydbo = templates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(5);
	            hybrid = templates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(6);
	            answerhead = templates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(7);
	            keywords = templates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(8);
	            if(experimentTaskName.toLowerCase().equals("hybrid")){
	            	hybridResult = templates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(9);
	            	hybridResult = hybridResult.replaceAll("&result&", ";");
	            }
        	}
    		
        	boolean legalQuery = false;
        	if(!experimentTaskName.toLowerCase().equals("hybrid")){
        		legalQuery = true;
        		result = "some result.";
				Query sparqlQuery = QueryFactory.create(query);
				QueryExecution qexec = QueryExecutionFactory.sparqlService(sparqlService, sparqlQuery);
				qexec.setTimeout(10000);
				try {
					if(questionWord.equals("is") || questionWord.equals("are")
							|| questionWord.equals("was") || questionWord.equals("were")
							|| questionWord.equals("did") || questionWord.equals("does") || questionWord.equals("do")
							|| questionWord.equals("can") || questionWord.equals("could")
							|| questionWord.equals("will") || questionWord.equals("would")){							
						boolean boolResult = qexec.execAsk();
						if(boolResult == true || boolResult == false) {
							legalQuery = true;
							result = Boolean.toString(boolResult);
						}										
					}
					else{
						Iterator<QuerySolution> results = qexec.execSelect();
						while(results.hasNext()) {
							legalQuery = true;
							QuerySolution querySolution = results.next();
							if(result.equals("")){
								result = querySolution.toString();
							}else{
								result = result+";"+querySolution.toString();
							}
						}
					}
				}
				catch(Exception e){
					String msg = "Exception while querying SPARQL endpoint "+sparqlService+". Aborting.";
					LOGGER.error(msg, e);
					throw new Exception(msg ,e);
				}
				finally {
					qexec.close();
				}
        	}else{legalQuery = true;}

	        if(legalQuery){
	        	byteArrayDataGenerator2TaskGenerator = RabbitMQUtils.writeString("");
	        	
	        	if(experimentTaskName.toLowerCase().equals("largescale")){
	        		byteArrayDataGenerator2TaskGenerator = RabbitMQUtils.writeString(pseudoRandomTemplate + "|" + question + "|" + query + "|" + result);
	        	}
	        	else if(experimentTaskName.toLowerCase().equals("multilingual")){
	        		switch(questionLanguage){
	        			case "fa":	question = templates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(9);
									keywords = templates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(10);
									break;
	        			case "de":	question = templates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(11);
									keywords = templates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(12);
									break;
	        			case "es":	question = templates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(13);
									keywords = templates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(14);
									break;
	        			case "it":	question = templates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(15);
									keywords = templates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(16);
									break;
	        			case "fr":	question = templates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(17);
									keywords = templates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(18);
									break;
	        			case "nl":	question = templates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(19);
									keywords = templates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(20);
									break;
	        			case "ro":	question = templates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(21);
									keywords = templates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(22);
									break;
	        		}
	        		byteArrayDataGenerator2TaskGenerator = RabbitMQUtils.writeString(pseudoRandomTemplate + "|" + question + "|" + query + "|" + result
	        				+ "|" + answertype + "|" + aggregation+ "|" + onlydbo + "|" + hybrid+ "|" + answerhead + "|" + keywords);
	    		}
	        	else if(experimentTaskName.toLowerCase().equals("hybrid")){
	        		byteArrayDataGenerator2TaskGenerator = RabbitMQUtils.writeString(pseudoRandomTemplate + "|" + question + "|" + query + "|" + hybridResult
	        				+ "|" + answertype + "|" + aggregation+ "|" + onlydbo + "|" + hybrid+ "|" + answerhead + "|" + keywords);
	        	}
 		        sendDataToTaskGenerator(byteArrayDataGenerator2TaskGenerator);
 	            queryCounter++;
 	            //TODO caching
	        }
	        else{
	        	String msg = "Query is not legal for: \""+question+"\"";
				LOGGER.error(msg);
				throw new Exception(msg);
	        }
	        if(queryCounter>=numberOfDocuments) { notEnoughDataGenerated = false; }
	        if(queryCounter%templatesSize==0 || usedTemplates.size()%templatesSize==0){
	        	usedTemplates.clear();
				usedTemplates.addAll(depletedTemplates);
	        }
	    	pseudoRandomTemplate = (pseudoRandomTemplate+pseudoRandomTemplate)%templatesSize;
		}
        templates.clear();
        usedQuestions.clear();
        usedTemplates.clear();
        depletedTemplates.clear();
        
        LOGGER.info("Data Generation finished.");
	}

    public void close() throws IOException {
    	LOGGER.info("Closing.");
        super.close();
        LOGGER.info("Closed.");
    }
}