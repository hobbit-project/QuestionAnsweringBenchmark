package org.hobbit.questionanswering.helper;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helper class for the GERBIL Question Answering Benchmarks
 */
public class QaHelper {
	
	public static final String META_MISSING = "metainfo.missing";
	public static final String EMPTY_RESULT = "EMPTY.RESULT";
	
	/**
	 * Overwrites QALD-String. Starting with head, containing the dataset id.
	 * (QALD-JSON-FORMAT)
	 * @param qaldString
	 * @param datasetId
	 * @return modified qaldString String
	 */
	public String addHead(String qaldString, String datasetId){
		qaldString += "{\n" +
  	   					"\"dataset\": {\n"+
  			   			"\"id\": \""+datasetId+"\"\n"+
  			   			"}"+
  			   			",\n"+
  			   			"\"questions\": [\n";
		return qaldString;
	}
	
	/**
	 * Adds the following parameters to the question part of the qaldString.
	 * (QALD-JSON-FORMAT)
	 * @param qaldString
	 * @param questionId
	 * @param answertype
	 * @param aggregation
	 * @param onlydbo
	 * @param hybrid
	 * @param language
	 * @param question
	 * @param keywords
	 * @return modified qaldString String
	 */
	public String addQuestionEvaluation(String qaldString, String questionId, String answertype, String aggregation,
	  		   String onlydbo, String hybrid, String language, String question, String keywords){
			qaldString += 	"{\n"+
	  			   			"\"id\": \""+questionId+"\",\n"+
	  			   			"\"answertype\": \""+answertype+"\",\n"+
	  			   			"\"aggregation\": \""+aggregation+"\",\n"+
	  			   			"\"onlydbo\": \""+onlydbo+"\",\n"+
	  			   			"\"hybrid\": \""+hybrid+"\",\n"+
	  			   			"\"question\": [\n{\n"+
	  			   			"\"language\": \""+language+"\",\n"+
	  			   			"\"string\": \"" +question+"\",\n"+
	  			   			"\"keywords\": \"" +keywords+"\"\n"+
	  			   			"}\n"+"],\n";
			return qaldString;
	}
	
	/**
	 * Adds the following parameters to the question part of the qaldString.
	 * (QALD-JSON-FORMAT)
	 * @param qaldString
	 * @param questionId
	 * @param answertype
	 * @param aggregation
	 * @param onlydbo
	 * @param hybrid
	 * @param language
	 * @param question
	 * @param keywords
	 * @return modified qaldString String
	 */
	public String addQuestionHybridEvaluation(String qaldString, String questionId, String answertype, String aggregation,
	  		   String onlydbo, String hybrid, String language, String question){
			qaldString += 	"{\n"+
	  			   			"\"id\": \""+questionId+"\",\n"+
	  			   			"\"answertype\": \""+answertype+"\",\n"+
	  			   			"\"aggregation\": \""+aggregation+"\",\n"+
	  			   			"\"onlydbo\": \""+onlydbo+"\",\n"+
	  			   			"\"hybrid\": \""+hybrid+"\",\n"+
	  			   			"\"question\": [\n{\n"+
	  			   			"\"language\": \""+language+"\",\n"+
	  			   			"\"string\": \"" +question+"\"\n"+
	  			   			"}\n"+"],\n";
			return qaldString;
	}
	
	/**
	 * Adds the following parameters to the question part of the qaldString. If language is not English, English is also added and sent to the Evaluation. Used for Multilingual Task.
	 * (QALD-JSON-FORMAT)
	 * @param qaldString
	 * @param questionId
	 * @param answertype
	 * @param aggregation
	 * @param onlydbo
	 * @param hybrid
	 * @param language
	 * @param question
	 * @param keywords
	 * @param englishLanguage
	 * @param englishQuestion
	 * @param englishKeywords
	 * @return modified qaldString String
	 */
	public String addQuestionMultilingualEvaluation(String qaldString, String questionId, String answertype, String aggregation,
	  		   String onlydbo, String hybrid, String language, String question, String keywords,
	  		   String englishLanguage, String englishQuestion, String englishKeywords){
			qaldString += 	"{\n"+
	  			   			"\"id\": \""+questionId+"\",\n"+
	  			   			"\"answertype\": \""+answertype+"\",\n"+
	  			   			"\"aggregation\": \""+aggregation+"\",\n"+
	  			   			"\"onlydbo\": \""+onlydbo+"\",\n"+
	  			   			"\"hybrid\": \""+hybrid+"\",\n"+
	  			   			"\"question\": [\n";	  			   			
			if(!language.equalsIgnoreCase("en")){
				qaldString +=
							"{\n"+
	  			   			"\"language\": \""+englishLanguage+"\",\n"+
	  			   			"\"string\": \"" +englishQuestion+"\",\n"+
	  			   			"\"keywords\": \"" +englishKeywords+"\"\n"+
	  			   			"},\n"; }
			qaldString +=
	  			   			"{\n"+
		  			   		"\"language\": \""+language+"\",\n"+
		  			   		"\"string\": \"" +question+"\",\n"+
		  			   		"\"keywords\": \"" +keywords+"\"\n"+
		  			   		"}\n"+"],\n";
			return qaldString;
	}
	
	/**
	 * Adds the following parameters to the question part of the qaldString. No query is following.
	 * (QALD-JSON-FORMAT)
	 * @param qaldString
	 * @param questionId
	 * @param answertype
	 * @param aggregation
	 * @param onlydbo
	 * @param hybrid
	 * @param language
	 * @param question
	 * @param keywords
	 * @return modified qaldString String
	 */
	public String addQuestionSystem(String qaldString, String questionId, String answertype, String aggregation,
	  		   String onlydbo, String hybrid, String language, String question, String keywords){
			qaldString += 	"{\n"+
	  			   			"\"id\": \""+questionId+"\",\n"+
	  			   			"\"answertype\": \""+answertype+"\",\n"+
	  			   			"\"aggregation\": \""+aggregation+"\",\n"+
	  			   			"\"onlydbo\": \""+onlydbo+"\",\n"+
	  			   			"\"hybrid\": \""+hybrid+"\",\n"+
	  			   			"\"question\": [\n{\n"+
	  			   			"\"language\": \""+language+"\",\n"+
	  			   			"\"string\": \"" +question+"\",\n"+
	  			   			"\"keywords\": \"" +keywords+"\"\n"+
	  			   			"}\n"+"]\n"+"}\n";
		return qaldString;
	}
	
	/**
	 * Adds the following parameters to the question part of the qaldString. No query is following. No keywords for hybrid.
	 * (QALD-JSON-FORMAT)
	 * @param qaldString
	 * @param questionId
	 * @param answertype
	 * @param aggregation
	 * @param onlydbo
	 * @param hybrid
	 * @param language
	 * @param question
	 * @return modified qaldString String
	 */
	public String addQuestionHybridSystem(String qaldString, String questionId, String answertype, String aggregation,
	  		   String onlydbo, String hybrid, String language, String question){
			qaldString += 	"{\n"+
	  			   			"\"id\": \""+questionId+"\",\n"+
	  			   			"\"answertype\": \""+answertype+"\",\n"+
	  			   			"\"aggregation\": \""+aggregation+"\",\n"+
	  			   			"\"onlydbo\": \""+onlydbo+"\",\n"+
	  			   			"\"hybrid\": \""+hybrid+"\",\n"+
	  			   			"\"question\": [\n{\n"+
	  			   			"\"language\": \""+language+"\",\n"+
	  			   			"\"string\": \"" +question+"\"\n"+
	  			   			"}\n"+"]\n"+"}\n";
		return qaldString;
	}
	
	/**
	 * Adds the following parameters to the query part of the qaldString.
	 * (QALD-JSON-FORMAT)
	 * @param qaldString
	 * @param sparql
	 * @return modified qaldString String
	 */
	public String addQuery(String qaldString, String sparql){
    	qaldString += "\"query\":{\n"+
  			   			"\"sparql\": \"" +sparql+"\"\n"+
  			   			"},\n";
    	return qaldString;
    }
	
	/**
	 * Adds the following parameters to the pseudoQuery part of the qaldString. Used for Hybrid Task.
	 * (QALD-JSON-FORMAT)
	 * @param qaldString
	 * @param sparql
	 * @return modified qaldString String
	 */
	public String addPseudoQuery(String qaldString, String sparql){
    	qaldString += "\"query\":{\n"+
  			   			"\"pseudo\": \"" +sparql+"\"\n"+
  			   			"},\n";
    	return qaldString;
    }
    
	/**
	 * Adds the following parameters to the answer part of the qaldString. Results are modified if varType is 'uri' or 'literal'.
	 * (QALD-JSON-FORMAT)
	 * @param qaldString
	 * @param queryReturn
	 * @param varType
	 * @param varValue
	 * @return modified qaldString String
	 */
	public String addAnswerModifyResult(String qaldString, String queryReturn, String varType, String varValue){
    	String newVarValue="";
    	
    	if(varValue.equals(EMPTY_RESULT)){
    		//newVarValue = varValue;
    		newVarValue = "";
    	}
    	else if(varType.equals("uri")){
    		newVarValue = getUriResult(varValue);
    	
		}else if(varType.equals("literal")){
			newVarValue = getLiteralResult(varValue);
			
		}else { newVarValue = varValue; }
    	
    	qaldString += "\"answers\": [\n{\n"+
  			   			"\"head\": {\n";
    	if(!varType.equals("boolean")) { qaldString +=
  			   			"\"vars\": [\n"+
  			   			"\""+queryReturn+"\""+"\n]\n},\n"+
  			   			"\"results\": {\n"+
  			   			"\"bindings\": [\n{\n"+
  			   			"\""+queryReturn+"\": {\n"+
  			   			"\"type\": \""+varType+"\",\n"+
  			   			"\"value\": \""+newVarValue+"\"\n}\n"+
  			   			"}\n]\n}\n}\n]\n}\n";}
    	else { qaldString +=
  			   			"},\n"+
  			   			"\""+varType+"\": "+newVarValue+"\n"+
  			   			"}\n]\n}\n";}
    	return qaldString;
    }
	
	/**
	 * Adds the following parameters to the answer part of the qaldString. Wikidata results are not modified.
	 * (QALD-JSON-FORMAT)
	 * @param qaldString
	 * @param queryReturn
	 * @param varType
	 * @param varValue
	 * @param datatype
	 * @return modified qaldString String
	 */
	public String addAnswer(String qaldString, String queryReturn, String varType, String varValue, String datatype){
    	String newVarValue=varValue;
    	
    	qaldString += "\"answers\": [\n{\n"+
  			   			"\"head\": {\n";
    	if(!varType.equals("boolean")) { qaldString +=
  			   			"\"vars\": [\n"+
  			   			"\""+queryReturn+"\""+"\n]\n},\n"+
  			   			"\"results\": {\n"+
  			   			"\"bindings\": [\n{\n"+
  			   			"\""+queryReturn+"\": {\n";
    	if(!datatype.equalsIgnoreCase(META_MISSING)){ qaldString +=
    					"\"datatype\": \""+datatype+"\",\n";}
    	qaldString += "\"type\": \""+varType+"\",\n"+
  			   			"\"value\": \""+newVarValue+"\"\n}\n"+
  			   			"}\n]\n}\n}\n]\n}\n";}
    	else { qaldString +=
  			   			"},\n"+
  			   			"\""+varType+"\": "+newVarValue+"\n"+
  			   			"}\n]\n}\n";}
    	return qaldString;
    }
    
	/**
	 * Adds the following parameters to the answer part of the qaldString. One answer consists of several results. Results are modified if varType is 'uri' or 'literal'.
	 * (QALD-JSON-FORMAT)
	 * @param qaldString
	 * @param queryReturn
	 * @param varType
	 * @param varValue
	 * @return modified qaldString String
	 */
    public String addMultipleAnswersModifyResult(String qaldString, String queryReturn, String varType, String[] varValue){
    	qaldString += "\"answers\": [\n{\n"+
  			   			"\"head\": {\n"+
  			   			"\"vars\": [\n"+
  			   			"\""+queryReturn+"\""+"\n]\n},\n"+
  			   			"\"results\": {\n"+
  			   			"\"bindings\": [\n";
    	
    	for(int r=0; r<(varValue.length)-1; r++){
    		String newVarValue="";

			if(varType.equals("uri")){
				newVarValue = getUriResult(varValue[r]);

			}else if(varType.equals("literal")){
				newVarValue = getLiteralResult(varValue[r]);

			}else { newVarValue = varValue[r]; }
			qaldString = qaldString
						+ "{\n"
						+ "\""+queryReturn+"\": {\n"
						+ "\"type\": \""+varType+"\",\n"
						+ "\"value\": \""+newVarValue+"\"\n}\n"
						+ "},\n";
		}
		String newVarValue="";
		if(varType.equals("uri")){
			newVarValue = getUriResult(varValue[varValue.length-1]);
		}else if(varType.equals("literal")){
			newVarValue = getLiteralResult(varValue[varValue.length-1]);
		}else { newVarValue = varValue[varValue.length-1]; }
		qaldString = qaldString	+ "{\n" + "\""+queryReturn+"\": {\n" + "\"type\": \""+varType+"\",\n" + "\"value\": \""+newVarValue+"\"\n}\n" + "}\n"
						+ "]\n}\n}\n]\n}\n";
		return qaldString;
	}
    
    /**
     * Adds the following parameters to the answer part of the qaldString. One answer consists of several results. Results are not modified. If available, datatype is added.
	 * (QALD-JSON-FORMAT)
     * @param qaldString
     * @param queryReturn
     * @param varType
     * @param varValue
     * @param datatype
     * @return modified qaldString String
     */
    public String addMultipleAnswers(String qaldString, String queryReturn, String varType, String[] varValue, String datatype){
    	qaldString += "\"answers\": [\n{\n"+
  			   			"\"head\": {\n"+
  			   			"\"vars\": [\n"+
  			   			"\""+queryReturn+"\""+"\n]\n},\n"+
  			   			"\"results\": {\n"+
  			   			"\"bindings\": [\n";
    	
    	for(int r=0; r<(varValue.length)-1; r++){
    		String newVarValue = varValue[r];

			qaldString = qaldString
						+ "{\n"
						+ "\""+queryReturn+"\": {\n";
			if(!datatype.equalsIgnoreCase(META_MISSING)){ qaldString +=
						"\"datatype\": \""+datatype+"\",\n";}
			qaldString +=
						"\"type\": \""+varType+"\",\n"
						+ "\"value\": \""+newVarValue+"\"\n}\n"
						+ "},\n";
		}
    	
		String newVarValue=varValue[(varValue.length)-1];
		qaldString = qaldString	+ "{\n" + "\""+queryReturn+"\": {\n";
		if(!datatype.equalsIgnoreCase(META_MISSING)){ qaldString +=
									"\"datatype\": \""+datatype+"\",\n";}
		qaldString = qaldString + "\"type\": \""+varType+"\",\n" + "\"value\": \""+newVarValue+"\"\n}\n" + "}\n"
						+ "]\n}\n}\n]\n}\n";
		return qaldString;
	}
    
    /**
     * Adds the foot brackets to the qaldString.
	 * (QALD-JSON-FORMAT)
     * @param qaldString
     * @return modified qaldString String
     */
    public String addFoot(String qaldString){
    	qaldString += "]\n"+ "}";
    	return qaldString;
    }
    
    /*****************************************************************************************************************/
    
    /**
     * Gets the URI result, which is surrounded by <...>
     * @param varValue
     * @return newVarValue
     */
    public String getUriResult(String varValue){
    	String newVarValue = "";
    	
    	Matcher m = Pattern.compile("<(.*)>").matcher(varValue);
		while (m.find()) {
			newVarValue = m.group(0);
		}
		if(!newVarValue.equals("")){ newVarValue = newVarValue.replace("<", "").replace(">", ""); }
		else{
			Matcher m2 = Pattern.compile("\"(.*)\"").matcher(varValue);
			while (m2.find()) {
				newVarValue = m2.group(0);
			}
			if(!newVarValue.equals("")){ newVarValue = newVarValue.replaceAll("\"", ""); }
			else{
				Matcher m3 = Pattern.compile("= (.*) ").matcher(varValue);
				while (m3.find()) {
					newVarValue = m3.group(0);
				}
				if(!newVarValue.equals("")){
					StringBuilder newString = new StringBuilder(newVarValue);
					newString.deleteCharAt(newVarValue.length()-1);
					newString.deleteCharAt(0);
					newString.deleteCharAt(0);
					newVarValue = newString.toString();
				}
			}
		}
		return newVarValue;
    }
	
    /**
     * Gets the Literal result which is surrounded by "..."
     * @param varValue
     * @return newVarValue
     */
	public String getLiteralResult(String varValue){
    	String newVarValue = "";
    	
    	Matcher m = Pattern.compile("\"(.*)\"").matcher(varValue);
    	while (m.find()) {
    		newVarValue = m.group(0);
    	}
		if(!newVarValue.equals("")){ newVarValue = newVarValue.replaceAll("\"", ""); }
		else{
			Matcher m2 = Pattern.compile("<(.*)>").matcher(varValue);
			while (m2.find()) {
				newVarValue = m2.group(0);
			}
			if(!newVarValue.equals("")){ newVarValue = newVarValue.replace("<", "").replace(">", ""); }
			else{
				Matcher m3 = Pattern.compile("= (.*) ").matcher(varValue);
				while (m3.find()) {
					newVarValue = m3.group(0);
				}
				if(!newVarValue.equals("")){
					StringBuilder newString = new StringBuilder(newVarValue);
					newString.deleteCharAt(newVarValue.length()-1);
					newString.deleteCharAt(0);
					newString.deleteCharAt(0);
					newVarValue = newString.toString();
				}
			}
		}
		return newVarValue;
    }
	
	/*****************************************************************************************************************/
	
	/**
	 * Reads in the existing question templates required for the given task.
	 * @param experimentTaskName
	 * @return templates
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public ArrayList<ArrayList<ArrayList<String>>> getTasks(String experimentTaskName) throws Exception{
        FileInputStream fis;
        ObjectInputStream ois = null;
        ArrayList<ArrayList<ArrayList<String>>> templates = null;
		try {
			String filepath = "data/"+experimentTaskName.toLowerCase()+".dat";
			fis = new FileInputStream(filepath);
			ois = new ObjectInputStream(new BufferedInputStream(fis));
			templates = (ArrayList<ArrayList<ArrayList<String>>>) ois.readObject();
		}catch(Exception e){
			e.printStackTrace();
		}
		ois.close();
		return templates;
    }
	
	/**
	 * Reads in the Large-scale sample values, containing meta-info for the Large-scale testing task.
	 * @return largescaleSampleValues
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public ArrayList<ArrayList<String>> getLargescaleSampleValues() throws Exception{
        FileInputStream fis;
        ObjectInputStream ois = null;
        ArrayList<ArrayList<String>> largescaleSampleValues = null;
		try {
			fis = new FileInputStream("data/largescalesamplevalues.dat");
			ois = new ObjectInputStream(new BufferedInputStream(fis));
			largescaleSampleValues = (ArrayList<ArrayList<String>>) ois.readObject();
		}catch(Exception e){
			e.printStackTrace();
		}
		ois.close();
		return largescaleSampleValues;
    }
}