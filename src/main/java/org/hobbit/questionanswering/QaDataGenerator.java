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
    public static final String NUMBER_OF_QUESTIONS_PARAMETER_KEY = "qa.number_of_questions";
    public static final String SEED_PARAMETER_KEY = "qa.seed";
    public static final String SPARQL_SERVICE_PARAMETER_KEY = "qa.sparql_service";
    public static final String DATASET_PARAMETER_KEY = "qa.dataset";
    
    public static final String RESULT_MISSING = "result.missing";
    public static final String META_EN_MISSING = "metainfo-en.missing";
    public static final String META_MISSING = "metainfo.missing";
    public static final String RESULT_EMPTY = "EMPTY.RESULT";
    
    public static final String MULTILINGUAL = "multilingual";
    public static final String HYBRID = "hybrid";
    public static final String LARGESCALE = "largescale";
    public static final String WIKIDATA = "wikidata";
    
    private String experimentTypeName;
    private String experimentTaskName;
    private String experimentDataset;
    private String questionLanguage;
    private String sparqlService;
    
    private int numberOfQuestions;
    private long seed;

    private QaHelper qaHelper = new QaHelper();
    private ArrayList<ArrayList<ArrayList<String>>> taskTemplates = null;
    
    /**
     * Initializes the Data Generator by getting all necessary environment parameters, which are set by the benchmark controller.
     */
    public void init() throws Exception {
    	LOGGER.info("QaDataGen: Initializing.");
    	super.init();
        Map<String, String> env = System.getenv();
        
        //load experimentTypeName from environment
        if(env.containsKey(EXPERIMENT_TYPE_PARAMETER_KEY)) {
        	String value = env.get(EXPERIMENT_TYPE_PARAMETER_KEY);
            try {
            	experimentTypeName = value;
            	LOGGER.info("QaDataGen: Got experiment type from the environment parameters: \""+experimentTypeName+"\"");
            } catch (Exception e) {
                LOGGER.error("QaDataGen: Exception while trying to parse the experiment type. Aborting.", e);
                throw new Exception("Exception while trying to parse the experiment type. Aborting.", e);
            }
        } else {
            String msg = "QaDataGen: Couldn't get \"" + EXPERIMENT_TYPE_PARAMETER_KEY + "\" from the properties. Aborting.";
            LOGGER.error(msg);
            throw new Exception(msg);
        }
        
        //load experimentTaskName from environment
        if(env.containsKey(EXPERIMENT_TASK_PARAMETER_KEY)) {
            String value = env.get(EXPERIMENT_TASK_PARAMETER_KEY);
            try {
            	experimentTaskName = value;
            	LOGGER.info("QaDataGen: Got experiment task from the environment parameters: \""+experimentTaskName+"\"");
            } catch (Exception e) {
                LOGGER.error("QaDataGen: Exception while trying to parse the experiment task. Aborting.", e);
                throw new Exception("Exception while trying to parse the experiment task. Aborting.", e);
            }
        } else {
            String msg = "QaDataGen: Couldn't get \"" + EXPERIMENT_TASK_PARAMETER_KEY + "\" from the properties. Aborting.";
            LOGGER.error(msg);
            throw new Exception(msg);
        }
        
        //load experimentDataset from environment
        if(env.containsKey(DATASET_PARAMETER_KEY)) {
            String value = env.get(DATASET_PARAMETER_KEY);
            try {
            	experimentDataset = value;
            	LOGGER.info("QaDataGen: Got dataset value from the environment parameters: \""+experimentDataset+"\"");
            } catch (Exception e) {
                LOGGER.error("QaDataGen: Exception while trying to parse the dataset value. Aborting.", e);
                throw new Exception("Exception while trying to parse the dataset value. Aborting.", e);
            }
        } else {
            String msg = "QaDataGen: Couldn't get \"" + DATASET_PARAMETER_KEY + "\" from the properties. Aborting.";
            LOGGER.error(msg);
            throw new Exception(msg);
        }
        
        //load tasks (+metainfo) for chosen task type
        LOGGER.info("QaDataGen: Loading taskdata (+metainfo) for "+experimentTaskName+"-"+experimentDataset+".");
        try{
        	if(experimentDataset.equalsIgnoreCase("testing")){
        		taskTemplates = qaHelper.getTasks(experimentTaskName.toLowerCase()+"testing");
        	}else{
        		taskTemplates = qaHelper.getTasks(experimentTaskName.toLowerCase()+"training");
        	}	
    	}catch(Exception e){
    		String msg = "QaDataGen: Exception while getting taskdata (+metainfo). Aborting.";
			LOGGER.error(msg, e);
			throw new Exception(msg, e);
    	}
        LOGGER.info("QaDataGen: Taskdata (+metainfo) for "+experimentTaskName+" loaded.");
        
        //load questionLanguage from environment
        if(env.containsKey(QUESTION_LANGUAGE_PARAMETER_KEY)) {
            String value = env.get(QUESTION_LANGUAGE_PARAMETER_KEY);
            try {
            	questionLanguage = value;
            	LOGGER.info("QaDataGen: Got language from the environment parameters: \""+questionLanguage+"\"");
            } catch (Exception e) {
                LOGGER.error("QaDataGen: Exception while trying to parse the experiment language. Aborting.", e);
                throw new Exception("Exception while trying to parse the experiment language. Aborting.", e);
            }
        } else {
            String msg = "QaDataGen: Couldn't get \"" + QUESTION_LANGUAGE_PARAMETER_KEY + "\" from the properties. Aborting.";
            LOGGER.error(msg);
            throw new Exception(msg);
        }
        
        //load number of questions from environment
        if(env.containsKey(NUMBER_OF_QUESTIONS_PARAMETER_KEY)){
        	try {
                numberOfQuestions = Integer.parseInt(env.get(NUMBER_OF_QUESTIONS_PARAMETER_KEY));
                LOGGER.info("QaDataGen: Got number of questions from the environment parameters: \""+numberOfQuestions+"\"");
            } catch (NumberFormatException e) {
            	LOGGER.error("QaDataGen: Exception while trying to parse the number of questions. Aborting.", e);
                throw new IllegalArgumentException("Exception while trying to parse the number of questions. Aborting.", e);
            }
        }else{
        	LOGGER.error("QaDataGen: Couldn't get \"" + NUMBER_OF_QUESTIONS_PARAMETER_KEY + "\" from the environment. Aborting.");
            throw new IllegalArgumentException("Couldn't get \"" + NUMBER_OF_QUESTIONS_PARAMETER_KEY + "\" from the environment. Aborting.");
        }
        if(!experimentTaskName.equalsIgnoreCase(LARGESCALE)){
        	if(numberOfQuestions>taskTemplates.size()){
        		numberOfQuestions = taskTemplates.size();
        		LOGGER.error("QaDataGen: Chosen number of questions is too high.");
        		LOGGER.info("QaDataGen: Reducing number of questions to "+numberOfQuestions+".");
        	}
        }
        
        //load seed from environment
        if(env.containsKey(SEED_PARAMETER_KEY)){
        	try {
                seed = Long.parseLong(env.get(SEED_PARAMETER_KEY));
                LOGGER.info("QaDataGen: Got seed from the environment parameters: \""+seed+"\"");
            } catch (NumberFormatException e) {
            	LOGGER.error("QaDataGen: Exception while trying to parse the seed. Aborting.", e);
                throw new IllegalArgumentException("Exception while trying to parse the seed. Aborting.", e);
            }
        }else{
        	LOGGER.error("QaDataGen: Couldn't get \"" + SEED_PARAMETER_KEY + "\" from the environment. Aborting.");
            throw new IllegalArgumentException("Couldn't get \"" + SEED_PARAMETER_KEY + "\" from the environment. Aborting.");
        }
        
        //load sparqlService from environment
        sparqlService = "";
        if(env.containsKey(SPARQL_SERVICE_PARAMETER_KEY)) {
            String value = env.get(SPARQL_SERVICE_PARAMETER_KEY);
            try {
            	sparqlService = value;
            	LOGGER.info("QaDataGen: Got SPARQL service from the environment parameters: \""+sparqlService+"\"");
            } catch (Exception e) {
                LOGGER.error("QaDataGen: Exception while trying to parse the SPARQL service. Aborting.", e);
                throw new Exception("Exception while trying to parse the SPARQL service. Aborting.", e);
            }
        } else {
            String msg = "QaDataGen: Couldn't get \"" + SPARQL_SERVICE_PARAMETER_KEY + "\" from the properties. Aborting.";
            LOGGER.error(msg);
            throw new Exception(msg);
        }
        
        LOGGER.info("QaDataGen: Initialized.");
    }

    /**
     * Generates amount of data, depending on <code>numberOfQuestions</code>.
     * Results for Large-scale are generated live.
     */
    public void generateData() throws Exception{
    	LOGGER.info("QaDataGen: Generating data and sending it to the Task Generator.");
    	
    	int templatesSize = 0;
    	int pseudoRandomTemplate = 0;
    	int pseudoRandomQuestion = 0;
    	int numberOfVariousQuestions = 0;
    	int queryCounter = 0;
    	
    	String question = "";
		String query = "";
		String result = "";
		
		byte[] byteArrayDataGenerator2TaskGenerator;
    	
    	templatesSize = taskTemplates.size();
		pseudoRandomTemplate = (int) (seed%templatesSize);
    	
    	ArrayList<String> usedQuestions = new ArrayList<String>();
    	ArrayList<Integer> usedTemplates = new ArrayList<Integer>();
    	if(experimentTaskName.toLowerCase().equals(LARGESCALE) && experimentDataset.equalsIgnoreCase("testing")){
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
				
	    		numberOfVariousQuestions = taskTemplates.get(pseudoRandomTemplate).size();
	    		
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
    		
			//for multilingual, additionally send english
			String englishQuestion, englishKeywords;
			englishQuestion = englishKeywords = META_EN_MISSING;
			
    		String questionWord = "";
    		question = taskTemplates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(1);
    		question = question.replaceAll("\"", "'");
    		englishQuestion = question;
        	questionWord = question.split(" ")[0].toLowerCase();
        	query = taskTemplates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(2);
        	//prevent errors in query while reading qald-format
        	if(!experimentTaskName.toLowerCase().equals(HYBRID)){
        		query = query.replaceAll("\"", "'");
        	}
			
        	//for all tasks, result is given (givenResult)
        	//but for large-scale testing --> result is calculated on-the-fly (result)
        	String answertype, aggregation, onlydbo, hybrid, answerhead, keywords, givenResult, wikidataDatatype;
        	answertype = aggregation = onlydbo = hybrid = answerhead = keywords = givenResult = wikidataDatatype = META_MISSING;
        	
        	boolean gotLegalResult = false;
        	
        	if(	(experimentDataset.equalsIgnoreCase("training")) ||
        		( experimentDataset.equalsIgnoreCase("testing") && !experimentTaskName.toLowerCase().equals(LARGESCALE) )){
        		gotLegalResult = true;
        		
	        	answertype = taskTemplates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(3);
	            aggregation = taskTemplates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(4);
	            onlydbo = taskTemplates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(5);
	            hybrid = taskTemplates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(6);
	            answerhead = taskTemplates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(7);
	            keywords = taskTemplates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(8);
	            englishKeywords = keywords;
	            givenResult = taskTemplates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(9);
	            givenResult = givenResult.replaceAll("&result&", ";");
	            
        	}
        	else{
        		result = RESULT_MISSING;
				Query sparqlQuery = QueryFactory.create(query);
				QueryExecution qexec = QueryExecutionFactory.sparqlService(sparqlService, sparqlQuery);
				qexec.setTimeout(50000);
				try {
					if(questionWord.equals("is") || questionWord.equals("are")
							|| questionWord.equals("was") || questionWord.equals("were")
							|| questionWord.equals("did") || questionWord.equals("does") || questionWord.equals("do")
							|| questionWord.equals("can") || questionWord.equals("could")
							|| questionWord.equals("will") || questionWord.equals("would")){							
						boolean boolResult = qexec.execAsk();
						if(boolResult == true || boolResult == false) {
							gotLegalResult = true;
							result = Boolean.toString(boolResult);
						}
					}
					else{
						Iterator<QuerySolution> results = qexec.execSelect();
						if(!results.hasNext()){
							//for empty ResultSet --> provide an empty String (done by the QaHelper)
							result = RESULT_EMPTY;
							gotLegalResult = true;
						}
						while(results.hasNext()) {
							gotLegalResult = true;
							QuerySolution querySolution = results.next();
							if(result.equalsIgnoreCase(RESULT_MISSING)){
								result = querySolution.toString();
							}else{
								result = result+";"+querySolution.toString();
							}
						}
					}
				}
				catch(Exception e){
					String msg = "QaDataGen: Exception while querying SPARQL endpoint "+sparqlService+". Aborting.";
					LOGGER.error(msg, e);
					throw new Exception(msg ,e);
				}
				finally {
					qexec.close();
				}
        	}

	        if(gotLegalResult){
	        	byteArrayDataGenerator2TaskGenerator = RabbitMQUtils.writeString("");
	        	
	        	if(experimentTaskName.toLowerCase().equals(MULTILINGUAL)){
	        		switch(questionLanguage){
	        			case "fa":	question = taskTemplates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(10);
									keywords = taskTemplates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(11);
									break;
	        			case "de":	question = taskTemplates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(12);
									keywords = taskTemplates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(13);
									break;
	        			case "es":	question = taskTemplates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(14);
									keywords = taskTemplates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(15);
									break;
	        			case "it":	question = taskTemplates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(16);
									keywords = taskTemplates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(17);
									break;
	        			case "fr":	question = taskTemplates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(18);
									keywords = taskTemplates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(19);
									break;
	        			case "nl":	question = taskTemplates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(20);
									keywords = taskTemplates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(21);
									break;
	        			case "ro":	question = taskTemplates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(22);
									keywords = taskTemplates.get(pseudoRandomTemplate).get(pseudoRandomQuestion).get(23);
									break;
	        		}
	        		byteArrayDataGenerator2TaskGenerator = RabbitMQUtils.writeString(pseudoRandomTemplate + "|" + question + "|" + query + "|" + givenResult
	        				+ "|" + answertype + "|" + aggregation+ "|" + onlydbo + "|" + hybrid+ "|" + answerhead + "|" + keywords
	        				+ "|" + englishQuestion + "|" + englishKeywords);
	    		}
	        	else if(experimentTaskName.toLowerCase().equals(HYBRID)){
	        		byteArrayDataGenerator2TaskGenerator = RabbitMQUtils.writeString(pseudoRandomTemplate + "|" + question + "|" + query + "|" + givenResult
	        				+ "|" + answertype + "|" + aggregation+ "|" + onlydbo + "|" + hybrid+ "|" + answerhead);
	        	}
	        	else if(experimentTaskName.toLowerCase().equals(LARGESCALE) && experimentDataset.equalsIgnoreCase("testing")){
	        		byteArrayDataGenerator2TaskGenerator = RabbitMQUtils.writeString(pseudoRandomTemplate + "|" + question + "|" + query + "|" + result);
	        	}
	        	else if(experimentTaskName.toLowerCase().equals(LARGESCALE) && experimentDataset.equalsIgnoreCase("training")){
	        		byteArrayDataGenerator2TaskGenerator = RabbitMQUtils.writeString(pseudoRandomTemplate + "|" + question + "|" + query + "|" + givenResult
	        				+ "|" + answertype + "|" + aggregation+ "|" + onlydbo + "|" + hybrid+ "|" + answerhead + "|" + keywords);
	        	}
	        	else if(experimentTaskName.toLowerCase().equals(WIKIDATA)){
	        		byteArrayDataGenerator2TaskGenerator = RabbitMQUtils.writeString(pseudoRandomTemplate + "|" + question + "|" + query + "|" + givenResult
	        				+ "|" + answertype + "|" + aggregation+ "|" + onlydbo + "|" + hybrid+ "|" + answerhead + "|" + keywords + "|" + wikidataDatatype);
	        	}
 		        sendDataToTaskGenerator(byteArrayDataGenerator2TaskGenerator);
 	            queryCounter++;
 	            //TODO caching
	        }
	        else{
	        	String msg = "QaDataGen: Didn't get legal result for: \""+question+"\"\nresult was "+result+"\nskipping ...";
				LOGGER.info(msg);
				throw new Exception(msg);
	        }
	        
	        //breaking conditions
	        if(queryCounter>=numberOfQuestions) { notEnoughDataGenerated = false; }
	        if(queryCounter%templatesSize==0 || usedTemplates.size()%templatesSize==0){
	        	usedTemplates.clear();
				usedTemplates.addAll(depletedTemplates);
	        }
	        
	    	pseudoRandomTemplate = (pseudoRandomTemplate+pseudoRandomTemplate)%templatesSize;
		}
		taskTemplates.clear();
        usedQuestions.clear();
        usedTemplates.clear();
        depletedTemplates.clear();
        
        LOGGER.info("QaDataGen: Data Generation finished.");
	}

    /**
     * Calls super.close() Method and logs Closing-Information.
     */
    public void close() throws IOException {
    	LOGGER.info("QaDataGen: Closing.");
        super.close();
        LOGGER.info("QaDataGen: Closed.");
    }
}