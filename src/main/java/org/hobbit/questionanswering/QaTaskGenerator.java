package org.hobbit.questionanswering;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.hobbit.core.components.AbstractTaskGenerator;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.hobbit.questionanswering.helper.QaHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QaTaskGenerator extends AbstractTaskGenerator{
	private static final Logger LOGGER = LoggerFactory.getLogger(QaTaskGenerator.class);
	
	public static final String EXPERIMENT_TYPE_PARAMETER_KEY = "qa.experiment_type";
	public static final String EXPERIMENT_TASK_PARAMETER_KEY = "qa.experiment_task";
	public static final String QUESTION_LANGUAGE_PARAMETER_KEY = "qa.question_language";
	public static final String NUMBER_OF_QUESTIONS_PARAMETER_KEY = "qa.number_of_questions";
	public static final String TIME_FOR_ANSWERING_PARAMETER_KEY = "qa.time_for_answering";
	public static final String SEED_PARAMETER_KEY = "qa.seed";
	
	public static final String RESULT_MISSING = "result.missing";
	public static final String RESULTTYPE_MISSING = "resulttype.missing";
    public static final String META_EN_MISSING = "metainfo-en.missing";
    public static final String META_MISSING = "metainfo.missing";
	
	public static final String MULTILINGUAL = "multilingual";
    public static final String HYBRID = "hybrid";
    public static final String LARGESCALE = "largescale";
    public static final String WIKIDATA = "wikidata";
    
	private String experimentTypeName;
	private String experimentTaskName;
    private String questionLanguage;
    private int numberOfQuestions;
    private long timeForAnswering;
	private long seed;
    private String datasetId;
    
    private QaHelper qaHelper = new QaHelper();
    ArrayList<ArrayList<String>> templateSampleValues = null;
    
    ArrayList<byte[]> taskDataList = null;
    ArrayList<byte[]> answerDataList = null;
    int taskCounter;

    public void init() throws Exception {
    	LOGGER.info("QaTaskGen: Initializing.");
    	super.init();
        Map<String, String> env = System.getenv();

        //load experimentTypeName from environment
        if(env.containsKey(EXPERIMENT_TYPE_PARAMETER_KEY)) {
        	String value = env.get(EXPERIMENT_TYPE_PARAMETER_KEY);
            try {
            	experimentTypeName = value;
            	LOGGER.info("QaTaskGen: Got experiment type from the environment parameters: \""+experimentTypeName+"\"");
            } catch (Exception e) {
                LOGGER.error("QaTaskGen: Exception while trying to parse the experiment type. Aborting.", e);
                throw new Exception("Exception while trying to parse the experiment type. Aborting.", e);
            }
        } else {
            String msg = "QaTaskGen: Couldn't get \"" + EXPERIMENT_TYPE_PARAMETER_KEY + "\" from the properties. Aborting.";
            LOGGER.error(msg);
            throw new Exception(msg);
        }
        
        //load experimentTaskName from environment
        if(env.containsKey(EXPERIMENT_TASK_PARAMETER_KEY)) {
            String value = env.get(EXPERIMENT_TASK_PARAMETER_KEY);
            try {
            	experimentTaskName = value;
            	LOGGER.info("QaTaskGen: Got experiment task from the environment parameters: \""+experimentTaskName+"\"");
            } catch (Exception e) {
                LOGGER.error("QaTaskGen: Exception while trying to parse the experiment task. Aborting.", e);
                throw new Exception("Exception while trying to parse the experiment task. Aborting.", e);
            }
        } else {
            String msg = "QaTaskGen: Couldn't get \"" + EXPERIMENT_TASK_PARAMETER_KEY + "\" from the properties. Aborting.";
            LOGGER.error(msg);
            throw new Exception(msg);
        }
        
        //load sample values for task type largescale
        if(experimentTaskName.equalsIgnoreCase(LARGESCALE)){
        	try{
        		LOGGER.info("QaTaskGen: Loading sample values for "+experimentTaskName+".");
    	    	templateSampleValues = qaHelper.getLargescaleSampleValues();
    	    	LOGGER.info("QaTaskGen: Sample values for "+experimentTaskName+" loaded.");
    		}catch(Exception e){
    			String msg = "QaTaskGen: Exception while getting sample data.";
    			LOGGER.error(msg, e);
    			throw new Exception(msg, e);
        	}
        }
        
        //load questionLanguage from environment
        if(env.containsKey(QUESTION_LANGUAGE_PARAMETER_KEY)) {
            String value = env.get(QUESTION_LANGUAGE_PARAMETER_KEY);
            try {
            	questionLanguage = value;
            	LOGGER.info("QaTaskGen: Got language from the environment parameters: \""+questionLanguage+"\"");
            } catch (Exception e) {
                LOGGER.error("QaTaskGen: Exception while trying to parse the experiment language. Aborting.", e);
                throw new Exception("Exception while trying to parse the experiment language. Aborting.", e);
            }
        } else {
            String msg = "QaTaskGen: Couldn't get \"" + QUESTION_LANGUAGE_PARAMETER_KEY + "\" from the properties. Aborting.";
            LOGGER.error(msg);
            throw new Exception(msg);
        }
        
        //load number of questions from environment
        if(env.containsKey(NUMBER_OF_QUESTIONS_PARAMETER_KEY)){
        	try {
                numberOfQuestions = Integer.parseInt(env.get(NUMBER_OF_QUESTIONS_PARAMETER_KEY));
                LOGGER.info("QaTaskGen: Got number of questions from the environment parameters: \""+numberOfQuestions+"\"");
            } catch (NumberFormatException e) {
            	LOGGER.error("QaTaskGen: Exception while trying to parse the number of questions. Aborting.", e);
                throw new IllegalArgumentException("Exception while trying to parse the number of questions. Aborting.", e);
            }
        }else{
        	LOGGER.error("QaTaskGen: Couldn't get \"" + NUMBER_OF_QUESTIONS_PARAMETER_KEY + "\" from the environment. Aborting.");
            throw new IllegalArgumentException("Couldn't get \"" + NUMBER_OF_QUESTIONS_PARAMETER_KEY + "\" from the environment. Aborting.");
        }
        
        //load time for answering from environment
        if(env.containsKey(TIME_FOR_ANSWERING_PARAMETER_KEY)){
        	try {
        		timeForAnswering = Long.parseLong(env.get(TIME_FOR_ANSWERING_PARAMETER_KEY));
                LOGGER.info("QaTaskGen: Got time for answering from the environment parameters: \""+timeForAnswering+"\"");
            } catch (NumberFormatException e) {
            	LOGGER.error("QaTaskGen: Exception while trying to parse the time for answering. Aborting.", e);
                throw new IllegalArgumentException("Exception while trying to parse the time for answering. Aborting.", e);
            }
        }else{
        	LOGGER.error("QaTaskGen: Couldn't get \"" + TIME_FOR_ANSWERING_PARAMETER_KEY + "\" from the environment. Aborting.");
            throw new IllegalArgumentException("Couldn't get \"" + TIME_FOR_ANSWERING_PARAMETER_KEY + "\" from the environment. Aborting.");
        }
        
        //load seed from environment
        if(env.containsKey(SEED_PARAMETER_KEY)){
        	try {
                seed = Long.parseLong(env.get(SEED_PARAMETER_KEY));
                LOGGER.info("QaTaskGen: Got seed from the environment parameters: \""+seed+"\"");
            } catch (NumberFormatException e) {
            	LOGGER.error("QaTaskGen: Exception while trying to parse the seed. Aborting.", e);
                throw new IllegalArgumentException("Exception while trying to parse the seed. Aborting.", e);
            }
        }else{
        	LOGGER.error("QaTaskGen: Couldn't get \"" + SEED_PARAMETER_KEY + "\" from the environment. Aborting.");
            throw new IllegalArgumentException("Couldn't get \"" + SEED_PARAMETER_KEY + "\" from the environment. Aborting.");
        }
        
        //datasetId
        datasetId = "hobbit_qa_"+this.getHobbitSessionId()+"_"+seed+"_"+experimentTaskName.toLowerCase();
        LOGGER.info("QaTaskGen: Dataset id is "+datasetId+".");
        
        taskCounter = 0;
        taskDataList = new ArrayList<byte[]>();
        answerDataList = new ArrayList<byte[]>();
        
        LOGGER.info("QaTaskGen: Initialized.");
    }

    protected void generateTask(byte[] data) throws Exception {
    	String taskId = getNextTaskId();
    	
    	String englishLanguage, englishQuestion, englishKeywords;
    	englishLanguage = englishQuestion = englishKeywords = META_EN_MISSING;
    	
    	String stringArray = RabbitMQUtils.readString(data);
		String[] qqr = stringArray.split("\\|");
		String questionString = qqr[1];
		String queryString = qqr[2];
		String resultString = qqr[3];
		int templateId = Integer.parseInt(qqr[0]);
		
		boolean singleAnswer = true;
		if(resultString.contains(";")) singleAnswer = false;
		
		String resulttype = RESULTTYPE_MISSING;
		String answertype, aggregation, onlydbo, hybrid, answerhead, keywords, datatype;
		answertype = aggregation = onlydbo = hybrid = answerhead = keywords = datatype = META_MISSING;
		
		if(experimentTaskName.toLowerCase().equals(LARGESCALE)){
			answertype = templateSampleValues.get(templateId).get(2);
			aggregation = templateSampleValues.get(templateId).get(3);
			onlydbo = templateSampleValues.get(templateId).get(4);
			hybrid = templateSampleValues.get(templateId).get(5);
			answerhead = "";
			resulttype = "unknown";
			if(!answertype.equals("boolean")) { answerhead = templateSampleValues.get(templateId).get(6); }
			else { resulttype = "boolean"; }
			if(answertype.equals("resource")) { resulttype = "uri"; }
			else if(!answertype.equals("") && !resulttype.equals("boolean") ) { resulttype = "literal"; }
			String toBeReplacedkeywords = templateSampleValues.get(templateId).get(7);

			String toBeReplacedString = templateSampleValues.get(templateId).get(0);
			String leftString = "";
			String rightString = "";
			Pattern leftSide = Pattern.compile("(.)*toBeReplacedKeyword");
			Pattern rightSide = Pattern.compile("toBeReplacedKeyword(.)*");
			Matcher m = leftSide.matcher(toBeReplacedString);
			if (m.find()) { leftString = m.group(0).replace("toBeReplacedKeyword", ""); }
			m = rightSide.matcher(toBeReplacedString);
			if (m.find()) { rightString = m.group(0).replace("toBeReplacedKeyword", ""); }
			String newKeyword = questionString.replace(leftString, "").replace(rightString, "");
			if(Character.isWhitespace(newKeyword.charAt(newKeyword.length()-1 ) ) ) { newKeyword = newKeyword.substring(0,newKeyword.length()-1); }
			keywords = toBeReplacedkeywords.replace("toBeReplacedKeyword", newKeyword);
			
		}else{
			answertype = qqr[4];
            aggregation = qqr[5];
            onlydbo = qqr[6];
            hybrid = qqr[7];
            answerhead = qqr[8];
            keywords = qqr[9];
            if(experimentTaskName.equalsIgnoreCase(MULTILINGUAL)){
            	englishLanguage = "en";
            	englishQuestion = qqr[10];
            	englishKeywords = qqr[11];
            }else if(experimentTaskName.equalsIgnoreCase(WIKIDATA)){
            	datatype = qqr[10];
            }
            if(answertype.equals("boolean")) { resulttype = "boolean"; }
			if(answertype.equals("resource")) { resulttype = "uri"; }
			else if(!answertype.equals("") && !resulttype.equals("boolean") ) { resulttype = "literal"; }
		}
		
		String qaldFormatStringToSystem = "";
		String qaldFormatStringToEvaluation="";
		qaldFormatStringToSystem = qaHelper.addHead(qaldFormatStringToSystem, datasetId);
		qaldFormatStringToEvaluation = qaHelper.addHead(qaldFormatStringToEvaluation, datasetId);
		
		if(experimentTaskName.toLowerCase().equals(HYBRID)){
        	qaldFormatStringToSystem = qaHelper.addQuestionSystem(qaldFormatStringToSystem, taskId, answertype, aggregation, onlydbo, hybrid, questionLanguage, questionString, keywords);
        	qaldFormatStringToSystem = qaHelper.addFoot(qaldFormatStringToSystem);
        	
        	qaldFormatStringToEvaluation = qaHelper.addQuestion(qaldFormatStringToEvaluation, taskId, answertype, aggregation, onlydbo, hybrid, questionLanguage, questionString, keywords);
        	qaldFormatStringToEvaluation = qaHelper.addPseudoQuery(qaldFormatStringToEvaluation, queryString);
        	if(singleAnswer){ qaldFormatStringToEvaluation = qaHelper.addAnswerHybrid(qaldFormatStringToEvaluation, answerhead, resulttype, resultString); }
			else{
				String[] results = resultString.split(";");
				qaldFormatStringToEvaluation = qaHelper.addMultipleAnswersHybrid(qaldFormatStringToEvaluation, answerhead, resulttype, results);
			}
        	qaldFormatStringToEvaluation = qaHelper.addFoot(qaldFormatStringToEvaluation);
        }
        else if(experimentTaskName.toLowerCase().equals(LARGESCALE)){
        	qaldFormatStringToSystem = qaHelper.addQuestionSystem(qaldFormatStringToSystem, taskId, answertype, aggregation, onlydbo, hybrid, questionLanguage, questionString, keywords);
        	qaldFormatStringToSystem = qaHelper.addFoot(qaldFormatStringToSystem);
        	
        	qaldFormatStringToEvaluation = qaHelper.addQuestion(qaldFormatStringToEvaluation, taskId, answertype, aggregation, onlydbo, hybrid, questionLanguage, questionString, keywords);
        	qaldFormatStringToEvaluation = qaHelper.addQuery(qaldFormatStringToEvaluation, queryString);
			if(singleAnswer) { qaldFormatStringToEvaluation = qaHelper.addAnswer(qaldFormatStringToEvaluation, answerhead, resulttype, resultString); }
			else{
				String[] results = resultString.split(";");
				qaldFormatStringToEvaluation = qaHelper.addMultipleAnswers(qaldFormatStringToEvaluation, answerhead, resulttype, results);
			}
			qaldFormatStringToEvaluation = qaHelper.addFoot(qaldFormatStringToEvaluation);
        }
        else if(experimentTaskName.toLowerCase().equals(MULTILINGUAL)){
        	qaldFormatStringToSystem = qaHelper.addQuestionSystem(qaldFormatStringToSystem, taskId, answertype, aggregation, onlydbo, hybrid, questionLanguage, questionString, keywords);
        	qaldFormatStringToSystem = qaHelper.addFoot(qaldFormatStringToSystem);
        	
        	qaldFormatStringToEvaluation = qaHelper.addQuestionMultilingualEvaluation(qaldFormatStringToEvaluation, taskId, answertype, aggregation, onlydbo, hybrid, questionLanguage, questionString, keywords, englishLanguage, englishQuestion, englishKeywords);
        	qaldFormatStringToEvaluation = qaHelper.addQuery(qaldFormatStringToEvaluation, queryString);
        	if(singleAnswer) { qaldFormatStringToEvaluation = qaHelper.addAnswerMultilingual(qaldFormatStringToEvaluation, answerhead, resulttype, resultString); }
			else{
				String[] results = resultString.split(";");
				qaldFormatStringToEvaluation = qaHelper.addMultipleAnswersMultilingual(qaldFormatStringToEvaluation, answerhead, resulttype, results);
			}
        	qaldFormatStringToEvaluation = qaHelper.addFoot(qaldFormatStringToEvaluation);
        }
        else if(experimentTaskName.toLowerCase().equals(WIKIDATA)){
        	qaldFormatStringToSystem = qaHelper.addQuestionSystem(qaldFormatStringToSystem, taskId, answertype, aggregation, onlydbo, hybrid, questionLanguage, questionString, keywords);
        	qaldFormatStringToSystem = qaHelper.addFoot(qaldFormatStringToSystem);
        	
        	qaldFormatStringToEvaluation = qaHelper.addQuestion(qaldFormatStringToEvaluation, taskId, answertype, aggregation, onlydbo, hybrid, questionLanguage, questionString, keywords);
        	qaldFormatStringToEvaluation = qaHelper.addQuery(qaldFormatStringToEvaluation, queryString);
        	if(singleAnswer) { qaldFormatStringToEvaluation = qaHelper.addAnswerWikidata(qaldFormatStringToEvaluation, answerhead, resulttype, resultString, datatype); }
			else{
				String[] results = resultString.split(";");
				qaldFormatStringToEvaluation = qaHelper.addMultipleAnswersWikidata(qaldFormatStringToEvaluation, answerhead, resulttype, results, datatype);
			}
        	qaldFormatStringToEvaluation = qaHelper.addFoot(qaldFormatStringToEvaluation);
        }

        byte[] taskData = RabbitMQUtils.writeString(qaldFormatStringToSystem);
        byte[] expectedAnswerData = RabbitMQUtils.writeString(qaldFormatStringToEvaluation);
        
        taskDataList.add(taskData);
        answerDataList.add(expectedAnswerData);
		//TODO caching
        
        // send data if numberOfQuestions reached
        taskCounter++;
        if(taskCounter == numberOfQuestions){
        	int amountOfQuestions = 1;
        	if(taskDataList.size() == answerDataList.size()){
        		LOGGER.info("QaTaskGen: Sending Task Data.");
                for(int i = 0; i<taskDataList.size(); i++){
                	
                	for(int t=0; t<amountOfQuestions; t++){
                		int internal_i = i+t;
                		String internal_taskId = String.valueOf(internal_i);
                		long timestamp = System.currentTimeMillis();
                    	sendTaskToSystemAdapter(internal_taskId, taskDataList.get(internal_i));
                    	sendTaskToEvalStorage(internal_taskId, timestamp, answerDataList.get(internal_i));
                    	if(t==amountOfQuestions-1) i+=t;
                	}
                	if(experimentTaskName.toLowerCase().equals(LARGESCALE)){
                		amountOfQuestions++;
                	}
                	LOGGER.info("QaTaskGen: Set of Task Data send. Waiting "+timeForAnswering+" milliseconds.");
                	TimeUnit.MILLISECONDS.sleep(timeForAnswering);
                }
                LOGGER.info("QaTaskGen: "+numberOfQuestions+" sets of Task Data send.");
                LOGGER.info("QaTaskGen: Sending Task Data and Answer Data finished.");
        	}
        	else{
        		String msg = "QaTaskGen: Generated amount of Answer Data does not fit to amount of Task Data.";
                LOGGER.error(msg);
                throw new Exception(msg);
        	}
        }
    }

    public void close() throws IOException {
    	LOGGER.info("QaTaskGen: Closing.");
        super.close();
        LOGGER.info("QaTaskGen: Closed.");
    }
}