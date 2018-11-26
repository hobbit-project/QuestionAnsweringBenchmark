package org.hobbit.questionanswering;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.hobbit.core.components.AbstractDataGenerator;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.hobbit.questionanswering.helper.QaHelper;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.jena.atlas.json.JsonValue;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;

/**
 * A class is inherits {@code AbstractDataGenerator} class to generate questions and answers and send them to {@code QaDataGenerator}.
 * 
 */
public class QaDataGenerator extends AbstractDataGenerator {
	private static final Logger LOGGER = LogManager.getLogger(QaDataGenerator.class);
	
	public static final String EXPERIMENT_TYPE_PARAMETER_KEY = "qa.experiment_type";
	public static final String EXPERIMENT_TASK_PARAMETER_KEY = "qa.experiment_task";
    public static final String QUESTION_LANGUAGE_PARAMETER_KEY = "qa.question_language";
    public static final String NUMBER_OF_QUESTIONS_PARAMETER_KEY = "qa.number_of_questions";
    public static final String SEED_PARAMETER_KEY = "qa.seed";
    public static final String SPARQL_SERVICE_PARAMETER_KEY = "qa.sparql_service";
    public static final String DATASET_PARAMETER_KEY = "qa.dataset";
    public static final String NUMBER_OF_TRIPLES_PARAMETER_KEY = "qa.number_of_triples";
    
    public static final String LARGESCALE = "largescale";
    public static final String MULTILINGUAL = "multilingual";
    
    public static final String TESTING = "testing";
    public static final String TRAINING = "training";
    
    private String experimentTypeName;
    private String experimentTaskName;
    private String questionLanguage;
    private int numberOfQuestionSets;
    private long seed;
    private String sparqlService;
    private String experimentDataset;
	private int numberOfTriples;
    
    private List<JsonValue> qaData;
    private QaHelper qaHelper;
    private int numberOfQuestions;
    Map<String, String> env;
    
    /**
     * Initializes the Data Generator by getting all necessary environment parameters, which are set by the benchmark controller.
     */
    public void init() throws Exception {
    	
    	Configurator.setRootLevel(Level.ALL);
    	
    	LOGGER.info("QaDataGen: Initializing.");
    	super.init();
    	//Get system environment information.
        env = System.getenv();
       
        /*
         * load experimentTypeName from environment
         * Ex: QA
         */
        if(env.containsKey(EXPERIMENT_TYPE_PARAMETER_KEY)) {
            try {
            	experimentTypeName =String.valueOf(env.get(EXPERIMENT_TYPE_PARAMETER_KEY));
            	LOGGER.info("QaDataGen: Got experiment type from the environment parameters: \""+experimentTypeName+"\"");
            } catch (Exception e) {
                throw this.localError("QaDataGen: Exception while trying to parse the experiment type. Aborting.", e);
            }
        } else {
            throw this.localError("QaDataGen: Couldn't get \"" + EXPERIMENT_TYPE_PARAMETER_KEY + "\" from the properties. Aborting.");
        }
        
        /*
         * load experimentTaskName from environment
         * Ex: LARGESCALE, MULTILINGUAL, WIKIDATA, WIKIDATA, or HYBRID
         */
        if(env.containsKey(EXPERIMENT_TASK_PARAMETER_KEY)) {
            try {
            	experimentTaskName = String.valueOf(env.get(EXPERIMENT_TASK_PARAMETER_KEY));
            	LOGGER.info("QaDataGen: Got experiment task from the environment parameters: \""+experimentTaskName+"\"");
            } catch (Exception e) {
                throw this.localError("QaDataGen: Exception while trying to parse the experiment task. Aborting.", e);
            }
        } else {
            throw this.localError("QaDataGen: Couldn't get \"" + EXPERIMENT_TASK_PARAMETER_KEY + "\" from the properties. Aborting.");
        }
        
        /*
         * load experimentDataset from environment
         * Ex: testing or training
         */
        if(env.containsKey(DATASET_PARAMETER_KEY)) {
            try {
            	experimentDataset = String.valueOf(env.get(DATASET_PARAMETER_KEY));
            	LOGGER.info("QaDataGen: Got dataset value from the environment parameters: \""+experimentDataset+"\"");
            } catch (Exception e) {
                throw this.localError("QaDataGen: Exception while trying to parse the dataset value. Aborting.", e);
            }
        } else {
            throw this.localError("QaDataGen: Couldn't get \"" + DATASET_PARAMETER_KEY + "\" from the properties. Aborting.");
        } 
        /*
         * load questionLanguage from environment
         * Ex: en, fr, or de
         */
        if(env.containsKey(QUESTION_LANGUAGE_PARAMETER_KEY)) {
            try {
            	questionLanguage = String.valueOf(env.get(QUESTION_LANGUAGE_PARAMETER_KEY));
            	LOGGER.info("QaDataGen: Got language from the environment parameters: \""+questionLanguage+"\"");
            } catch (Exception e) {
                throw this.localError("QaDataGen: Exception while trying to parse the experiment language. Aborting.", e);
            }
        } else {
            throw this.localError("QaDataGen: Couldn't get \"" + QUESTION_LANGUAGE_PARAMETER_KEY + "\" from the properties. Aborting.");
        }
        
        //load number of question sets from environment
        if(env.containsKey(NUMBER_OF_QUESTIONS_PARAMETER_KEY)){
        	try {
        		numberOfQuestionSets = Integer.parseInt(env.get(NUMBER_OF_QUESTIONS_PARAMETER_KEY));
                LOGGER.info("QaDataGen: Got number of questions sets from the environment parameters: \""+numberOfQuestionSets+"\"");
            } catch (NumberFormatException e) {
            	throw this.localErrorIllegal("QaDataGen: Exception while trying to parse the number of questions. Aborting.", e);
            }
        }else{
        	throw this.localErrorIllegal("QaDataGen: Couldn't get \"" + NUMBER_OF_QUESTIONS_PARAMETER_KEY + "\" from the environment. Aborting.");
        }
        
     // Load number of Triples
        if(env.containsKey(NUMBER_OF_TRIPLES_PARAMETER_KEY)){
        	try {
                numberOfTriples = Integer.parseInt(env.get(NUMBER_OF_TRIPLES_PARAMETER_KEY));
                LOGGER.info("QaDataGen: Got number of Triples from the environment parameters: \""+numberOfTriples+"\"");
            } catch (NumberFormatException e) {
            	throw this.localError("QaDataGen: Exception while trying to parse the number of Triples. Aborting.", e);
            }
        }else{
        	throw this.localErrorIllegal("QaDataGen: Couldn't get \"" + NUMBER_OF_TRIPLES_PARAMETER_KEY + "\" from the environment. Aborting.");
        }
        
        //Set numberOfQuetions
        if(experimentTaskName.equalsIgnoreCase(LARGESCALE) && experimentDataset.equalsIgnoreCase(TESTING))
        	this.numberOfQuestions = (this.numberOfQuestionSets*(this.numberOfQuestionSets+1))/2;
        else
        	this.numberOfQuestions = this.numberOfQuestionSets;
        
        //load seed from environment
        if(env.containsKey(SEED_PARAMETER_KEY)){
        	try {
                seed = Long.parseLong(env.get(SEED_PARAMETER_KEY));
                LOGGER.info("QaDataGen: Got seed from the environment parameters: \""+seed+"\"");
            } catch (NumberFormatException e) {
            	throw this.localErrorIllegal("QaDataGen: Exception while trying to parse the seed. Aborting.",e);
            }
        }else{
        	throw this.localErrorIllegal("QaDataGen: Couldn't get \"" + SEED_PARAMETER_KEY + "\" from the environment. Aborting.");
        }
        
        //load sparqlService from environment
        sparqlService = "";
        if(env.containsKey(SPARQL_SERVICE_PARAMETER_KEY)) {
            try {
            	sparqlService = String.valueOf(env.get(SPARQL_SERVICE_PARAMETER_KEY));
            	LOGGER.info("QaDataGen: Got SPARQL service from the environment parameters: \""+sparqlService+"\"");
            } catch (Exception e) {
                throw this.localError("QaDataGen: Exception while trying to parse the SPARQL service. Aborting.", e);
            }
        } else {
            throw this.localError("QaDataGen: Couldn't get \"" + SPARQL_SERVICE_PARAMETER_KEY + "\" from the properties. Aborting.");
        }

        /*
         * load tasks (+metainfo) for chosen task type
         * It load data from .json files by using qaHelper class
         */
        LOGGER.info("QaDataGen: Loading data (+metainfo) for "+experimentTaskName+"-"+experimentDataset+".");
        qaHelper=new QaHelper(this.seed,this.numberOfQuestions,this.sparqlService);
        try{
        	if(experimentDataset.equalsIgnoreCase(TRAINING)) {
        		switch(experimentTaskName) {
        		case LARGESCALE:
        			if(numberOfTriples==-1)
        				qaData=qaHelper.getLargeScaleData("data/largescale_training.json");
        			else
        				qaData = qaHelper.getLargeScaleData("data/ls_testing_num.json", numberOfTriples);
        			break;
        		case MULTILINGUAL:
        			qaData=qaHelper.getMultilingualData("data/multilingual_testing.json",questionLanguage);
        			break;
        		default:
        			throw this.localError("QaDataGen: Not supported Task!");
        		}
        	}else if(experimentDataset.equalsIgnoreCase(TESTING)) {
        		switch(experimentTaskName) {
        		case LARGESCALE:
        			if(numberOfTriples==-1)
        				qaData=qaHelper.getLargeScaleData("data/largescale_testing.json");
        			else
        				qaData = qaHelper.getLargeScaleData("data/ls_testing_num.json", numberOfTriples);
        			break;
        		case MULTILINGUAL:
        			qaData=qaHelper.getMultilingualData("data/multilingual_testing.json",questionLanguage);
        			break;
        		default:
        			throw this.localError("QaDataGen: Not supported Task yet!");
        		}
        	}else {
        		throw this.localError("QaDataGen: Data set must be only training or testing!");
        	}
    	}catch(Exception e){
    		throw this.localError("QaDataGen: Exception while getting data (+metainfo). Aborting.", e);
    	}
        LOGGER.info("QaDataGen: "+experimentTaskName+" data is loaded.");
        
        
        
      //Readjusts number of questions to equal
        if(numberOfQuestions>qaData.size()){
    		numberOfQuestions = qaData.size();
    		LOGGER.error("QaDataGen: Chosen number of questions is too high.");
    		LOGGER.info("QaDataGen: Reducing number of questions to "+numberOfQuestions+".");
        }
        LOGGER.info("QaDataGen: "+this.numberOfQuestions+" questions generated.");
        LOGGER.info("QaDataGen: Initialized.");
    }

    /**
     * Generates amount of data, depending on the number of questions that sent by Benchmark controller
     * and sent them to Task generator
     */
    public void generateData() throws Exception{
    	LOGGER.info("QaDataGen: Generating data and sending it to the Task Generator.");
    	for(int i=0;i<qaData.size();i++) {
    		sendDataToTaskGenerator(RabbitMQUtils.writeString(qaData.get(i).toString()));
    		//LOGGER.info(qaData.get(i));
    	}
    	LOGGER.info("QaDataGen: Data Generated and sent to task generator.");
	}

    /**
     * Calls super.close() Method and logs Closing-Information.
     */
    public void close() throws IOException {
    	LOGGER.info("QaDataGen: Closing.");
        super.close();
        LOGGER.info("QaDataGen: Closed.");
    }
    /**
	 * Customized error
	 * @param msg
	 * @return
	 */
	public Exception localError(String msg) {
		LOGGER.error(msg);
		return new Exception(msg);
	}
	public Exception localError(String msg,Throwable e) {
		LOGGER.error(msg,e);
		return new Exception(msg,e);
	}
	public IllegalArgumentException localErrorIllegal(String msg, Throwable e) {
		LOGGER.error(msg, e);
        return new IllegalArgumentException(msg, e);
	}
	public IllegalArgumentException localErrorIllegal(String msg) {
		LOGGER.error(msg);
        return new IllegalArgumentException(msg);
	}
}