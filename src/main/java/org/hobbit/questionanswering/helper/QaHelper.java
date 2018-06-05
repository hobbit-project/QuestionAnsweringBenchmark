package org.hobbit.questionanswering.helper;

import java.util.ArrayList;
import java.util.Random;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonArray;
import org.apache.jena.atlas.json.JsonBuilder;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSetFormatter;

public class QaHelper {
	
	private Random rand;
	private ArrayList<Integer> used;
	private ArrayList<String> data;

	private int numOfQuestions=0;
	private String sparqlService="http://dbpedia.org/sparql";
	
	public QaHelper(long seed,int numOfQuestions,String sparqlService) {
		this.numOfQuestions=numOfQuestions;
		this.sparqlService = sparqlService;
		
		this.data=new ArrayList<String>();
		this.used=new ArrayList<Integer>();
		this.rand=new Random(seed);
	}
	
	public  ArrayList<String> getLargeScaleData(String fileName) {
		//largescaledata.json
		JsonArray questionsArray=JSON.readAny("data/"+fileName).getAsArray();
		JsonBuilder newJson=new JsonBuilder();
		JsonObject randomlySelected;
		int randNum=-1,datasetSize=questionsArray.size();
		String query="";
		while(data.size()<this.numOfQuestions) {
			randNum=rand.nextInt(datasetSize);
			if(!used.contains(randNum)) {
				randomlySelected=questionsArray.get(randNum).getAsObject();
				randomlySelected.remove("_id");
				if(randomlySelected.hasKey("answers"))
					randomlySelected.remove("answers");
				query = randomlySelected.get("query").getAsObject().get("sparql").toString();
				newJson.startArray().value(getAnswers(query).get("answers")).finishArray();
				randomlySelected.put("answers", newJson.build());
				newJson.startObject()
					.key("questions").startArray()
						.value(randomlySelected)
					.finishArray()
				.finishObject();
				data.add(newJson.build().toString());
				used.add(randNum);
			}
		}
		return this.data;
	}
	
	public  ArrayList<String> getMultilingualData(String fileName,String lang) {
		//JsonValue allData=JSON.readAny("data/multilang_final_data_set_50.json");
		int langID = getLangID(lang);
		JsonArray questionsArray=JSON.readAny("data/"+fileName).getAsArray();
		JsonBuilder newJson=new JsonBuilder();
		JsonObject randomlySelected;
		int randNum=-1,datasetSize=questionsArray.size();
		String query="",question="";
		while(data.size()<this.numOfQuestions) {
			randNum=rand.nextInt(datasetSize);
			if(!used.contains(randNum)) {
				randomlySelected=questionsArray.get(randNum).getAsObject();
				
				query = randomlySelected.get("query").getAsArray().get(langID).getAsObject().get("sparql").toString().trim().replace("\"", "");
				newJson.startObject().key("sparql").value(query.trim()).finishObject();
				randomlySelected.put("query", newJson.build());
				
				question = randomlySelected.get("question").getAsArray().get(langID).getAsObject().get("string").toString().replace("\"", "");
				newJson.startArray().startObject().key("string").value(question)
				.key("language").value(lang).finishObject().finishArray();
				randomlySelected.put("question", newJson.build());
				
				if(randomlySelected.hasKey("answers"))
					randomlySelected.remove("answers");
				newJson.startArray().value(getAnswers(query).get("answers")).finishArray();
				randomlySelected.put("answers", newJson.build());
				
				newJson.startObject()
					.key("questions").startArray()
						.value(randomlySelected)
					.finishArray()
				.finishObject();
				data.add(newJson.build().toString());
				used.add(randNum);
			}
		}
		return this.data;
	}
	
	private String getAnswerType(JsonObject answerTypeObject) {
		String result="";
		if(answerTypeObject.hasKey("link"))
			result="boolean";
		else if(answerTypeObject.hasKey("vars")) {
			result="resource";
		}
		return result;
	}
	
	private JsonObject getAnswers(String query) {
		//"http://dbpedia.org/sparql"
		query = query.trim().replace("\"", "");
		QueryExecution qexec = QueryExecutionFactory.sparqlService(this.sparqlService, query);
		//qexec.setTimeout(2000);
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		
		if(query.startsWith("ASK WHERE")) {
			ResultSetFormatter.outputAsJSON(outputStream, qexec.execAsk());
		}else {
			ResultSetFormatter.outputAsJSON(outputStream,qexec.execSelect());
		}
		JsonObject jsonResult = new JsonObject();
		jsonResult.put("answers",JSON.parse(outputStream.toString()));
		return jsonResult;
	}
	
	private static int getLangID(String lang) {
    	int langID=0;
    	switch (lang) {
		case "en":
			langID = 0;
			break;
		case "de":
			langID = 1;
			break;
		case "it":
			langID = 2;
			break;
		default:
			break;
		}
		return langID;
    }
}