package org.hobbit.questionanswering;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.hobbit.core.components.AbstractTaskGenerator;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.hobbit.QaldBuilder;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;

public class QaTaskGenerator extends AbstractTaskGenerator{
	private static final Logger LOGGER = LogManager.getLogger(QaTaskGenerator.class);
	
	public static final String EXPERIMENT_TYPE_PARAMETER_KEY = "qa.experiment_type";
	public static final String EXPERIMENT_TASK_PARAMETER_KEY = "qa.experiment_task";
	public static final String NUMBER_OF_QUESTIONS_PARAMETER_KEY = "qa.number_of_questions";
	public static final String TIME_FOR_ANSWERING_PARAMETER_KEY = "qa.time_for_answering";
	public static final String SEED_PARAMETER_KEY = "qa.seed";
	public static final String DATASET_PARAMETER_KEY = "qa.dataset";
	
    public static final String LARGESCALE = "largescale";
    public static final String MULTILINGUAL = "multilingual";
    
    public static final String TESTING = "testing";
    public static final String TRAINING = "training";
    
	private String experimentTypeName;
	private String experimentTaskName;
	private int numberOfQuestionSets;
	private long timeForAnswering;
	private long seed;
	private String experimentDataset;
    
    private String datasetId;
    private long timestamp;
    private ArrayList<byte[]> taskDataList;
    private ArrayList<byte[]> answerDataList;
    private int taskCounter;
    private int numberOfQuestions;
    Map<String, String> env;
    QaldBuilder qaldQuestion;

    /**
     * Initializes the Task Generator by getting all necessary environment parameters, which are set by the benchmark controller.
     */
    public void init() throws Exception {
    	
    	Configurator.setRootLevel(Level.ALL); // configure logger
    	
    	LOGGER.info("QaTaskGen: Initializing.");
    	super.init(); // call initialisation function on super class
    	
    	env = System.getenv(); //Get system environment information.
    	//qaldQuestion = new QaldBuilder();
        /*
         * load experimentTypeName from environment
         * Ex: QA
         */
        if(env.containsKey(EXPERIMENT_TYPE_PARAMETER_KEY)) {
        	try {
            	experimentTypeName = String.valueOf(env.get(EXPERIMENT_TYPE_PARAMETER_KEY));
            	LOGGER.info("QaTaskGen: Got experiment type from the environment parameters: \""+experimentTypeName+"\"");
            } catch (Exception e) {
                throw this.localError("QaTaskGen: Exception while trying to parse the experiment type. Aborting.", e);
            }
        } else {
            throw this.localError("QaTaskGen: Couldn't get \"" + EXPERIMENT_TYPE_PARAMETER_KEY + "\" from the properties. Aborting.");
        }
        
        /*
         * load experimentTaskName from environment
         * Ex: LARGESCALE, MULTILINGUAL, WIKIDATA, WIKIDATA, or HYBRID
         */
        if(env.containsKey(EXPERIMENT_TASK_PARAMETER_KEY)) {
            try {
            	experimentTaskName =String.valueOf(env.get(EXPERIMENT_TASK_PARAMETER_KEY));
            	LOGGER.info("QaTaskGen: Got experiment task from the environment parameters: \""+experimentTaskName+"\"");
            } catch (Exception e) {
                throw this.localError("QaTaskGen: Exception while trying to parse the experiment task. Aborting.", e);
            }
        } else {
            throw this.localError("QaTaskGen: Couldn't get \"" + EXPERIMENT_TASK_PARAMETER_KEY + "\" from the properties. Aborting.");
        }
        
        /*
         * load experimentDataset from environment
         * Ex: testing or training
         */
        if(env.containsKey(DATASET_PARAMETER_KEY)) {
            try {
            	experimentDataset = String.valueOf(env.get(DATASET_PARAMETER_KEY));
            	LOGGER.info("QaTaskGen: Got dataset value from the environment parameters: \""+experimentDataset+"\"");
            } catch (Exception e) {
                throw this.localError("QaTaskGen: Exception while trying to parse the dataset value. Aborting.", e);
            }
        } else {
            throw this.localError("QaTaskGen: Couldn't get \"" + DATASET_PARAMETER_KEY + "\" from the properties. Aborting.");
        }
        
        //load number of questions from environment
        if(env.containsKey(NUMBER_OF_QUESTIONS_PARAMETER_KEY)){
        	try {
                numberOfQuestionSets = Integer.parseInt(env.get(NUMBER_OF_QUESTIONS_PARAMETER_KEY));
                LOGGER.info("QaTaskGen: Got number of questions from the environment parameters: \""+numberOfQuestionSets+"\"");
            } catch (NumberFormatException e) {
            	throw this.localError("QaTaskGen: Exception while trying to parse the number of questions. Aborting.", e);
            }
        }else{
        	throw this.localErrorIllegal("QaTaskGen: Couldn't get \"" + NUMBER_OF_QUESTIONS_PARAMETER_KEY + "\" from the environment. Aborting.");
        }
        
        //Set numberOfQuetions
        if(experimentTaskName.equalsIgnoreCase(LARGESCALE) && experimentDataset.equalsIgnoreCase(TESTING))
        	this.numberOfQuestions = (this.numberOfQuestionSets*(this.numberOfQuestionSets+1))/2;
        else
        	this.numberOfQuestions = this.numberOfQuestionSets;
        
        LOGGER.info("QaTaskGen: Nubmer of questions: "+this.numberOfQuestions);
        //load time for answering from environment
        if(env.containsKey(TIME_FOR_ANSWERING_PARAMETER_KEY)){
        	try {
        		timeForAnswering = Long.parseLong(env.get(TIME_FOR_ANSWERING_PARAMETER_KEY));
                LOGGER.info("QaTaskGen: Got time for answering from the environment parameters: \""+timeForAnswering+"\"");
            } catch (NumberFormatException e) {
            	throw this.localErrorIllegal("QaTaskGen: Couldn't get \"" + NUMBER_OF_QUESTIONS_PARAMETER_KEY + "\" from the environment. Aborting.", e);
            }
        }else{
        	throw this.localErrorIllegal("QaTaskGen: Couldn't get \"" + TIME_FOR_ANSWERING_PARAMETER_KEY + "\" from the environment. Aborting.");
        }
        
        /*
         * load seed from environment
         * To name the containers and dataset ID (AFAIK)
         */
        if(env.containsKey(SEED_PARAMETER_KEY)){
        	try {
                seed = Long.parseLong(env.get(SEED_PARAMETER_KEY));
                LOGGER.info("QaTaskGen: Got seed from the environment parameters: \""+seed+"\"");
            } catch (NumberFormatException e) {
            	throw this.localErrorIllegal("QaTaskGen: Exception while trying to parse the seed. Aborting.", e);
            }
        }else{
        	throw this.localErrorIllegal("QaTaskGen: Couldn't get \"" + SEED_PARAMETER_KEY + "\" from the environment. Aborting.");
        }
        
        //datasetId (hobbit_qa_1498123456789_42_largescale_training)
        datasetId = "hobbit_qa_"+this.getHobbitSessionId()+"_"+seed+"_"+experimentTaskName.toLowerCase()+"_"+experimentDataset.toLowerCase();
        LOGGER.info("QaTaskGen: Dataset id is "+datasetId+".");
        
        /*
         * load sample values for task type largescale
         * If benchmark is largescale, load largescaleSampleValues
         */
        try{
        	if(experimentTaskName.equalsIgnoreCase(LARGESCALE)
        			|| experimentTaskName.equalsIgnoreCase(MULTILINGUAL)){
            	LOGGER.info("QaTaskGen: Benshmark is supported "+experimentTaskName+".");
            }else {
            	throw this.localError(experimentTaskName+" Unsupported yet!");
            }
		}catch(Exception e){
			throw this.localError("QaTaskGen: Exception while getting sample data.", e);
    	}
        
        taskCounter = 0;
        taskDataList = new ArrayList<byte[]>();
        answerDataList = new ArrayList<byte[]>();
        
        LOGGER.info("QaTaskGen: Initialized.");
    }

    /**
     * Splits the received byte-Array into needed (meta-)informations and put them into the QALD-JSON-FORMAT as a String
     * depending on chosen task.
     * If all data is obtained, tasks will be sent to the system and evaluation storage with an interval of <code>timeForAnswering</code>.
     */
    protected void generateTask(byte[] data) throws Exception {
    	//String taskId = getNextTaskId();
    	qaldQuestion = new QaldBuilder(RabbitMQUtils.readString(data));
    	qaldQuestion.setDatasetID(this.datasetId);
    	answerDataList.add(RabbitMQUtils.writeString(qaldQuestion.getQuestionAsQald().toString()));
    	//LOGGER.info("With answers:\n"+qaldQuestion.getQaldQuestion());
    	qaldQuestion.removeAnswers();
    	qaldQuestion.removeQuery();
    	//LOGGER.info("Without answers:\n"+qaldQuestion.getQaldQuestion());
		taskDataList.add(RabbitMQUtils.writeString(qaldQuestion.getQuestionAsQald().toString()));
        // send data if numberOfQuestions reached
        taskCounter++;
        if(taskCounter == numberOfQuestions){
        	LOGGER.info("QaTaskGen: Num of tasks recieved equal num of Qs = "+taskCounter);
        	if(taskDataList.size() == answerDataList.size()){
        		LOGGER.info("QaTaskGen: Sending Task Data.");
        		if(experimentDataset.equalsIgnoreCase(TESTING) && experimentTaskName.equalsIgnoreCase(LARGESCALE)) {
        			int i=0;
            		for(int x=1;x<=this.numberOfQuestionSets;x++) {
                		for(int j=0;j<x;j++) {
                			sendData(i);
                			//LOGGER.info("QaTaskGen:Task "+i+" has been sent!");
                			i++;
                		}
            	    	TimeUnit.MILLISECONDS.sleep(timeForAnswering);
                	}
                }else {
            		for(int i = 0; i<numberOfQuestions; i++){
	                	sendData(i);
	                	//LOGGER.info("QaTaskGen: "+i+" has been sent!");
	                	TimeUnit.MILLISECONDS.sleep(timeForAnswering);
                	}
                }
	            LOGGER.info("QaTaskGen: "+numberOfQuestions+" sets of Task Data have being sent.");
	            LOGGER.info("QaTaskGen: Sending Task Data and Answer Data finished.");
        	}else{
        		throw this.localError("QaTaskGen: Generated amount of Answer Data does not fit to amount of Task Data.");
        	}
        }
    }

    /**
     * A function to send data to the system under testing and evaluation model
     * @param id = Task ID
     * @throws Exception
     */
    private void sendData(int id) throws Exception {
    	try {
	    	String internal_taskId = String.valueOf(id);
			timestamp = System.currentTimeMillis();
	    	sendTaskToSystemAdapter(internal_taskId, taskDataList.get(id));
	    	sendTaskToEvalStorage(internal_taskId, timestamp, answerDataList.get(id));
    	}catch(Exception e) {
    		throw this.localError("QaTaskGen: Can't send data!", e);
    	}
	}
	/**
     * Calls super.close() Method and logs Closing-Information.
     */
    public void close() throws IOException {
    	LOGGER.info("QaTaskGen: Closing.");
        super.close();
        LOGGER.info("QaTaskGen: Closed.");
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