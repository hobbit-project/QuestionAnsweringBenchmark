package org.hobbit.questionanswering;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
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
	public static final String SEED_PARAMETER_KEY = "qa.seed";
    
	private String experimentTypeName;
	private String experimentTaskName;
    private String questionLanguage;
	private long seed;
    private String datasetId;
    
    private QaHelper qaHelper = new QaHelper();
    ArrayList<ArrayList<String>> templateSampleValues = null;

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
        
        //load sample values for task type largescale
        if(experimentTaskName.equalsIgnoreCase("largescale")){
        	try{
        		LOGGER.info("Loading sample values for "+experimentTaskName+".");
    	    	templateSampleValues = qaHelper.getLargescaleSampleValues();
    	    	LOGGER.info("Sample values for "+experimentTaskName+" loaded.");
    		}catch(Exception e){
    			String msg = "Exception while getting sample data.";
    			LOGGER.error(msg, e);
    			throw new Exception(msg, e);
        	}
        }
        
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
        
        //datasetId
        datasetId = "hobbit_qa_"+this.getHobbitSessionId()+"_"+seed+"_"+experimentTaskName.toLowerCase();
        LOGGER.info("Dataset id is "+datasetId+".");
        
        LOGGER.info("Initialized.");
    }

    protected void generateTask(byte[] data) throws Exception {
    	String taskId = getNextTaskId();
    	
    	String stringArray = RabbitMQUtils.readString(data);
		String[] qqr = stringArray.split("\\|");
		String questionString = qqr[1];
		String queryString = qqr[2];
		String resultString = qqr[3];
		int templateId = Integer.parseInt(qqr[0]);
		
		boolean singleAnswer = true;
		if(resultString.contains(";")) singleAnswer = false;
		
		String resulttype = "resulttype.missing";
		String answertype, aggregation, onlydbo, hybrid, answerhead, keywords;
		answertype = aggregation = onlydbo = hybrid = answerhead = keywords = "metainfo.missing";
		
		if(experimentTaskName.toLowerCase().equals("largescale")){
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
            if(answertype.equals("boolean")) { resulttype = "boolean"; }
			if(answertype.equals("resource")) { resulttype = "uri"; }
			else if(!answertype.equals("") && !resulttype.equals("boolean") ) { resulttype = "literal"; }
		}
		
		String qaldFormatStringToSystem = "";
		String qaldFormatStringToEvaluation="";
		qaldFormatStringToSystem = qaHelper.addHead(qaldFormatStringToSystem, datasetId);
		qaldFormatStringToEvaluation = qaHelper.addHead(qaldFormatStringToEvaluation, datasetId);
		
		if(experimentTaskName.toLowerCase().equals("hybrid")){
        	qaldFormatStringToSystem = qaHelper.addQuestionSystem(qaldFormatStringToSystem, taskId, answertype, aggregation, onlydbo, hybrid, questionLanguage, questionString, keywords);
        	qaldFormatStringToSystem = qaHelper.addFoot(qaldFormatStringToSystem);
        	
        	qaldFormatStringToEvaluation = qaHelper.addQuestion(qaldFormatStringToEvaluation, taskId, answertype, aggregation, onlydbo, hybrid, questionLanguage, questionString, keywords);
        	qaldFormatStringToEvaluation = qaHelper.addQuery(qaldFormatStringToEvaluation, queryString);
        	if(singleAnswer){ qaldFormatStringToEvaluation = qaHelper.addAnswer(qaldFormatStringToEvaluation, answerhead, resulttype, resultString); }
			else{
				String[] results = resultString.split(";");
				qaldFormatStringToEvaluation = qaHelper.addMultipleAnswers(qaldFormatStringToEvaluation, answerhead, resulttype, results);
			}
        	qaldFormatStringToEvaluation = qaHelper.addFoot(qaldFormatStringToEvaluation);
        }
        else if(experimentTaskName.toLowerCase().equals("largescale")){
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
        else if(experimentTaskName.toLowerCase().equals("multilingual")){
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

        byte[] taskData = RabbitMQUtils.writeString(qaldFormatStringToSystem);
        byte[] expectedAnswerData = RabbitMQUtils.writeString(qaldFormatStringToEvaluation);
        
        sendTaskToSystemAdapter(taskId, taskData);
        long timestamp = System.currentTimeMillis();
        sendTaskToEvalStorage(taskId, timestamp, expectedAnswerData);
		//TODO caching
    }

    public void close() throws IOException {
    	LOGGER.info("Closing.");
        super.close();
        LOGGER.info("Closed.");
    }
}