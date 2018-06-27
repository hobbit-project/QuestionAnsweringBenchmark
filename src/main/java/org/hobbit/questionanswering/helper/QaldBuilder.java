package org.hobbit.questionanswering.helper;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonBuilder;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSetFormatter;

public class QaldBuilder {
	JsonObject qaldFormat,questionObject;
	JsonBuilder jsonBuilder;
	String query;
	
	public QaldBuilder() {
		jsonBuilder = new JsonBuilder();
		jsonBuilder.startObject()
			.key("dataset").startObject().finishObject()
			.key("questions").startArray()
				.startObject().finishObject().finishArray()
		.finishObject();
		qaldFormat = jsonBuilder.build().getAsObject();
		this.questionObject = this.qaldFormat.get("questions").getAsArray().get(0).getAsObject();
		/*
		this.setAggregation(true);
		this.setHybrid("false");
		this.setOnlydbo(true);
		*/
	}
	
	public void setQuestionAsJson(String question) {
		this.questionObject=JSON.parse(question);
		if(this.questionObject.hasKey("_id"))
			this.questionObject.remove("_id");
		
		this.setQuery(this.questionObject.get("query").getAsObject().get("sparql").toString());
	}
	
	public void setQuery(String query) {
		query = query.trim().replace("\"", "");
		if(this.questionObject.hasKey("query"))
			this.questionObject.remove("query");
		this.questionObject.put("query", query);
		this.query=query;
	}
	
	public void setAnswers(JsonObject answers) {
		if(this.questionObject.hasKey("answers"))
			this.questionObject.remove("answers");
		jsonBuilder.startArray().value(answers).finishArray();
		this.questionObject.put("answers", jsonBuilder.build());
	}
	
	public void setAnswers(String sparqlService) {
		if(this.questionObject.hasKey("answers"))
			this.questionObject.remove("answers");
		//"http://dbpedia.org/sparql"
		QueryExecution qexec = QueryExecutionFactory.sparqlService(sparqlService, query);
		//qexec.setTimeout(2000);
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		
		if(query.startsWith("ASK WHERE")) {
			ResultSetFormatter.outputAsJSON(outputStream, qexec.execAsk());
		}else {
			ResultSetFormatter.outputAsJSON(outputStream,qexec.execSelect());
		}
		this.questionObject.put("answers", JSON.parse(outputStream.toString()));
	}
	
	public void setQuestionString(String question,String language) {
		language=language.trim().replace("\"", "");
		if(this.questionObject.hasKey("question"))
			this.questionObject.remove("question");
		jsonBuilder.startArray().startObject()
			.key("string").value(question)
			.key("language").value(language)
		.finishObject().finishArray();
		this.questionObject.put("question", jsonBuilder.build());
	}
	
	public void setHybrid(String value) {
		if(this.questionObject.hasKey("hybrid"))
			this.questionObject.remove("hybrid");
		this.questionObject.put("hybrid", value);
	}
	
	public void setOnlydbo(boolean value) {
		if(this.questionObject.hasKey("onlydbo"))
			this.questionObject.remove("onlydbo");
		this.questionObject.put("onlydbo", value);
	}
	
	public void setAggregation(boolean value) {
		if(this.questionObject.hasKey("aggregation"))
			this.questionObject.remove("aggregation");
		this.questionObject.put("aggregation", value);
	}
	
	public void setID(int value) {
		if(this.questionObject.hasKey("id"))
			this.questionObject.remove("id");
		this.questionObject.put("id", value);
	}
	
	public String getQaldQuestion() {
		this.qaldFormat.get("questions").getAsArray().clear();
		this.qaldFormat.get("questions").getAsArray().add(this.questionObject);
		return this.qaldFormat.toString();
	}
}
