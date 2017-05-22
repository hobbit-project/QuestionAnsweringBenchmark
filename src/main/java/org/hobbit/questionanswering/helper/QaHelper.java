package org.hobbit.questionanswering.helper;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QaHelper {
	
	public String addHead(String extendedQald, String datasetId){
		extendedQald += "{\n" +
  	   					"\"dataset\": {\n"+
  			   			"\"id\": \""+datasetId+"\"\n"+
  			   			"}"+
  			   			",\n"+
  			   			"\"questions\": [\n";
		return extendedQald;
	}
	
	public String addQuestion(String extendedQald, String questionId, String answertype, String aggregation,
	  		   String onlydbo, String hybrid, String language, String question, String keywords){
			extendedQald += 	"{\n"+
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
			return extendedQald;
	}
	
	public String addQuestionSystem(String extendedQald, String questionId, String answertype, String aggregation,
	  		   String onlydbo, String hybrid, String language, String question, String keywords){
			extendedQald += 	"{\n"+
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
		return extendedQald;
	}
	
	public String addQuery(String extendedQald, String sparql){
    	extendedQald += "\"query\":{\n"+
  			   			"\"sparql\": \"" +sparql+"\"\n"+
  			   			"},\n";
    	return extendedQald;
    }
    
	public String addAnswer(String extendedQald, String queryReturn, String varType, String varValue){
    	String newVarValue="";
    	
    	if(varType.equals("uri")){
    		newVarValue = getUriResult(varValue);
    	
		}else if(varType.equals("literal")){
			newVarValue = getLiteralResult(varValue);
			
		}else { newVarValue = varValue; }
    	
    	extendedQald += "\"answers\": [\n{\n"+
  			   			"\"head\": {\n";
    	if(!varType.equals("boolean")) { extendedQald +=
  			   			"\"vars\": [\n"+
  			   			"\""+queryReturn+"\""+"\n]\n},\n"+
  			   			"\"results\": {\n"+
  			   			"\"bindings\": [\n{\n"+
  			   			"\""+queryReturn+"\": {\n"+
  			   			"\"type\": \""+varType+"\",\n"+
  			   			"\"value\": \""+newVarValue+"\"\n}\n"+
  			   			"}\n]\n}\n}\n]\n}\n";}
    	else { extendedQald +=
  			   			"},\n"+
  			   			"\""+varType+"\": "+newVarValue+"\n"+
  			   			"}\n]\n}\n";}
    	return extendedQald;
    }
    
    public String addMultipleAnswers(String extendedQald, String queryReturn, String varType, String[] varValue){
    	extendedQald += "\"answers\": [\n{\n"+
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
			extendedQald = extendedQald
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
		extendedQald = extendedQald	+ "{\n" + "\""+queryReturn+"\": {\n" + "\"type\": \""+varType+"\",\n" + "\"value\": \""+newVarValue+"\"\n}\n" + "}\n"
						+ "]\n}\n}\n]\n}\n";
		return extendedQald;
	}
    
    public String addFoot(String extendedQald){
    	extendedQald += "]\n"+ "}";
    	return extendedQald;
    }
    
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
	
	@SuppressWarnings("unchecked")
	public ArrayList<ArrayList<ArrayList<String>>> getTemplates(String experimentTaskName) throws Exception{
        FileInputStream fis;
        ObjectInputStream ois = null;
        ArrayList<ArrayList<ArrayList<String>>> templates = null;
		try {
			String filepath = "data/"+experimentTaskName.toLowerCase()+"Templates.dat";
			fis = new FileInputStream(filepath);
			ois = new ObjectInputStream(new BufferedInputStream(fis));
			templates = (ArrayList<ArrayList<ArrayList<String>>>) ois.readObject();
		}catch(Exception e){
			e.printStackTrace();
		}
		ois.close();
		return templates;
    }
	
	@SuppressWarnings("unchecked")
	public ArrayList<ArrayList<String>> getLargescaleSampleValues() throws Exception{
        FileInputStream fis;
        ObjectInputStream ois = null;
        ArrayList<ArrayList<String>> largescaleSampleValues = null;
		try {
			fis = new FileInputStream("data/largescaleSampleValues.dat");
			ois = new ObjectInputStream(new BufferedInputStream(fis));
			largescaleSampleValues = (ArrayList<ArrayList<String>>) ois.readObject();
		}catch(Exception e){
			e.printStackTrace();
		}
		ois.close();
		return largescaleSampleValues;
    }
}