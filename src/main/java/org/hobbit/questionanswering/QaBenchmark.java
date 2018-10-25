package org.hobbit.questionanswering;

import java.io.IOException;

import org.aksw.gerbil.datatypes.ExperimentType;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.rdf.model.NodeIterator;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.hobbit.core.Commands;
import org.hobbit.core.components.AbstractBenchmarkController;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;



/**
 * A class inherits AbstractBenchmarkController to control benchmarking system
 * @author Mohammed Abdelgadir 
 * @version 3.0.5
 */
public class QaBenchmark extends AbstractBenchmarkController {
	
	//private static final Logger LOGGER = LoggerFactory.getLogger(QaBenchmark.class);
	private static final Logger LOGGER = LogManager.getLogger(QaBenchmark.class);
	
	//Setup images links on HOBBIT repositories.
	private static final String DATA_GENERATOR_CONTAINER_IMAGE = "git.project-hobbit.eu:4567/weekmo/qadatagenv3a";
	private static final String TASK_GENERATOR_CONTAINER_IMAGE = "git.project-hobbit.eu:4567/weekmo/qataskgenv3a";
	private static final String EVALUATION_MODULE_CONTAINER_IMAGE = "git.project-hobbit.eu:4567/conrads/qaevaluationmodule";
	
	//private static  String EVALUATION_MODULE_CONTAINER_IMAGE = "git.project-hobbit.eu:4567/cmartens/qaevaluationmodule";
	protected static final String gerbilUri = "http://w3id.org/gerbil/vocab#";
	protected static final String gerbilQaUri = "http://w3id.org/gerbil/qa/hobbit/vocab#";
	protected static final Resource QA = resource("QA");
	protected static final Resource HYBRID = qaResource("hybridTask");
	protected static final Resource LARGESCALE = qaResource("largescaleTask");
	protected static final Resource MULTILINGUAL = qaResource("multilingualTask");
	protected static final Resource WIKIDATA = qaResource("wikidataTask");
	protected static final Resource TESTING = qaResource("testing");
	protected static final Resource TRAINING = qaResource("training");
	protected static final Resource ENGLISH = qaResource("EnLanguage");
	protected static final Resource FARSI = qaResource("FaLanguage");
	protected static final Resource GERMAN = qaResource("DeLanguage");
	protected static final Resource SPANISH = qaResource("EsLanguage");
	protected static final Resource ITALIAN = qaResource("ItLanguage");
	protected static final Resource FRENCH = qaResource("FrLanguage");
	protected static final Resource DUTCH = qaResource("NlLanguage");
	protected static final Resource ROMANIAN = qaResource("RoLanguage");
	protected static final Resource ONE_TRIPLE = qaResource("one");
	protected static final Resource TWO_TRIPLES = qaResource("two");
	protected static final Resource THREE_TRIPLES = qaResource("three");
	protected static final Resource NO_TRIPLES = qaResource("NoTriple");
	
	private final String _LARGESCALE="largescale";
	private final String _MULTILINGUAL="multilingual";
	//private final String _WIKIDATA="wikidata";
	//private final String _HYBRID="hybrid";
	
	private ExperimentType experimentType;
	
	private String experimentTaskName;
	private String experimentDataset;
	private String questionLanguage;
	private String sparqlService;
	private int numberOfTriples;
	
	private int numberOfQuestionSets;
	//private int numberOfQuestions;
	private long timeForAnswering;
	private long seed;
	
	private long startTime;
	
	//create single data and task generator
	private final int NUMBER_OF_GENERATORS = 1;
	
	/**
	 * Setup gerbil resource
	 * @param local
	 * @return Resource
	 */
	protected static final Resource resource(String local) {
        return ResourceFactory.createResource(gerbilUri + local);
    }
	
	/**
	 * Setup QA resource
	 * @param local
	 * @return Resource
	 */
	protected static final Resource qaResource(String local) {
        return ResourceFactory.createResource(gerbilQaUri + local);
    }

	/**
	 * Initializes the Benchmark Controller by reading and checking all necessary information from the benchmark model.
	 * Ex: parameters from web GUI
	 */
    @Override
    public void init() throws Exception {
    	
    	startTime = System.currentTimeMillis(); // a variable to carry starting time
    	Configurator.setRootLevel(Level.ALL); // setup logger
    	
    	LOGGER.info("QaBenchmark: Initializing.");
    	super.init(); // call intialisation function in super class
    	experimentType = ExperimentType.QA; // set experiment type to Question Answering
    	LOGGER.info("QaBenchmark: Loading parameters from benchmark model.");
        
    	/*
    	 * Set task type (largescale, multilingual, wikidata, hybrid)
    	 */
        NodeIterator iterator = benchmarkParamModel.listObjectsOfProperty(benchmarkParamModel.getProperty(gerbilQaUri+"hasExperimentTask"));
        if (iterator.hasNext()) {
            try {
            	Resource resource = iterator.next().asResource();
            	if (resource == null) {
            		throw this.localError("QaBenchmark: Got null resource.");
            	}else {
            		String uri = resource.getURI();
            		//LOGGER.info(uri);
	            	if (LARGESCALE.getURI().equals(uri)) {
	                    experimentTaskName = _LARGESCALE;
	                }else if (MULTILINGUAL.getURI().equals(uri)) {
	                    experimentTaskName = _MULTILINGUAL;
	                }
	            	/*
	                else if (WIKIDATA.getURI().equals(uri)) {
	                    experimentTaskName = "wikidata";
	                }else if (HYBRID.getURI().equals(uri)) {
	                    experimentTaskName = "hybrid";
	                }
	                */
	            	else {
	            		throw this.localError("QaBenchmark: The experiment task is not supported yet.");
	                }
	                LOGGER.info("QaBenchmark: Got experiment task from the parameter model: \""+experimentTaskName+"\"");
            	}
            } catch (Exception e) {
                LOGGER.error("QaBenchmark: Exception while parsing parameter.\n", e);
            }
        }
        
        //load Dataset from benchmark model
        iterator = benchmarkParamModel.listObjectsOfProperty(benchmarkParamModel.getProperty(gerbilQaUri+"hasDataset"));
        if (iterator.hasNext()) {
            try {
            	Resource resource = iterator.next().asResource();
            	if (resource == null){ 
            		throw this.localError("QaBenchmark: Got null resource.");
            	}else {
	            	String uri = resource.getURI();
	            	if (TESTING.getURI().equalsIgnoreCase(uri)) {
	            		experimentDataset = "testing";
	                }else if (TRAINING.getURI().equalsIgnoreCase(uri)) {
	            		experimentDataset = "training";
	                }else {
	                	throw this.localError("QaBenchmark: Dataset can be only testing or training");
	                }
	                LOGGER.info("QaBenchmark: Got experiment dataset from the parameter model: \""+experimentDataset+"\"");
            	}
            } catch (Exception e) {
                LOGGER.error("QaBenchmark: Exception while parsing parameter.\n", e);
            }
        }

        //load questionLanguage from benchmark model
        questionLanguage = "";
        if (!experimentTaskName.equalsIgnoreCase(_MULTILINGUAL)) {
        	questionLanguage = "en";
        	LOGGER.info("QaBenchmark: The language is sat to \"en\" due to experiment type is not \"multilingual\".");
        }else{
        	iterator = benchmarkParamModel.listObjectsOfProperty(benchmarkParamModel.getProperty(gerbilQaUri+"hasQuestionLanguage"));
        	if (iterator.hasNext()) {
                try {
                	Resource resource = iterator.next().asResource();
                	if (resource == null) { 
                		throw this.localError("QaBenchmark: Got null resource.");
                	}else {
	                	String uri = resource.getURI();
	                	if (ENGLISH.getURI().equals(uri)) {
	                        questionLanguage = "en";
	                    }else if (FARSI.getURI().equals(uri)) {
	                        questionLanguage = "fa";
	                    }else if (GERMAN.getURI().equals(uri)) {
	                        questionLanguage = "de";
	                    }else if (SPANISH.getURI().equals(uri)) {
	                        questionLanguage = "es";
	                    }else if (ITALIAN.getURI().equals(uri)) {
	                        questionLanguage = "it";
	                    }else if (FRENCH.getURI().equals(uri)) {
	                        questionLanguage = "fr";
	                    }else if (DUTCH.getURI().equals(uri)) {
	                        questionLanguage = "nl";
	                    }else if (ROMANIAN.getURI().equals(uri)) {
	                        questionLanguage = "ro";
	                    }else{
	                    	this.localError("QaBenchmark: Chosen question language is not supported yet");
	                    }
	                    LOGGER.info("QaBenchmark: Got question language from the parameter model: \""+questionLanguage+"\"");
                	}
                } catch (Exception e) {
                    LOGGER.error("QaBenchmark: Exception while parsing parameter.\n", e);
                }
            }
        }
        // Load triples
        numberOfTriples = -1;
        if (!experimentTaskName.equalsIgnoreCase(_LARGESCALE)) {
	        iterator = benchmarkParamModel.listObjectsOfProperty(benchmarkParamModel.getProperty(gerbilQaUri+"hasTriples"));
	    	if (iterator.hasNext()) {
	            try {
	            	Resource resource = iterator.next().asResource();
	            	if (resource == null) { 
	            		throw this.localError("QaBenchmark: Got null Triples.");
	            	}else {
	                	String uri = resource.getURI();
	                	if (ONE_TRIPLE.getURI().equals(uri)) {
	                        numberOfTriples =1;
	                    }else if (TWO_TRIPLES.getURI().equals(uri)) {
	                    	numberOfTriples =2;
	                    }else if (THREE_TRIPLES.getURI().equals(uri)) {
	                    	numberOfTriples =3;
	                    }else if (NO_TRIPLES.getURI().equals(uri)) {
	                    	numberOfTriples =-1;
	                    }else{
	                    	this.localError("QaBenchmark: There are only three possible options.");
	                    }
	                    LOGGER.info("QaBenchmark: Got nubmer of triples from the parameter model: \""+questionLanguage+"\"");
	            	}
	            } catch (Exception e) {
	                LOGGER.error("QaBenchmark: Exception while parsing parameter.\n", e);
	            }
	        }
        }
        
        //load numberOfQuestionSets from benchmark model
        numberOfQuestionSets = -1;
        iterator = benchmarkParamModel.listObjectsOfProperty(benchmarkParamModel.getProperty(gerbilQaUri+"hasNumberOfQuestionSets"));
        if(iterator.hasNext()) {
        	try {
        		numberOfQuestionSets = iterator.next().asLiteral().getInt();
                LOGGER.info("QaBenchmark: Got number of question sets from the parameter model: \""+numberOfQuestionSets+"\"");
            } catch (Exception e) {
                LOGGER.error("QaBenchmark: Exception while parsing parameter.\n", e);
            }
        }
        //check numberOfQuestionSets, set numberOfQuestions
        //If the number of questions set wrong.
        if (numberOfQuestionSets <= 0) {
        	LOGGER.error("QaBenchmark: Couldn't get the number of question sets from the parameter model. Using default value.");
        	//If it is large scale and testing set it to 30 by default.
        	if(experimentTaskName.equals(_LARGESCALE) && experimentDataset.equalsIgnoreCase("testing")){
        		numberOfQuestionSets = 30;
        	}else{
        		numberOfQuestionSets = 50;
        	}
        	LOGGER.info("QaBenchmark: Setting number of question sets to default value: \""+numberOfQuestionSets+"\"");
        }
        
        //load timeForAnswering from benchmark model
        timeForAnswering = -1;
        iterator = benchmarkParamModel.listObjectsOfProperty(benchmarkParamModel.getProperty(gerbilQaUri+"hasAnsweringTime"));
        if(iterator.hasNext()) {
        	try {
        		timeForAnswering = iterator.next().asLiteral().getLong();
                LOGGER.info("QaBenchmark: Got time for answering one question set from the parameter model: \""+timeForAnswering+"\"");
            } catch (Exception e) {
                LOGGER.error("QaBenchmark: Exception while parsing parameter.", e);
            }
        }
        /*
         * check timeForAnswering
         * if the time set wrong, put the default value.
         */
        if (timeForAnswering < 0) {
        	LOGGER.error("QaBenchmark: Couldn't get the time for answering one question set from the parameter model. Using default value.");
        	timeForAnswering = 60000;
        	LOGGER.info("QaBenchmark: Setting time for answering one question set to default value: \""+timeForAnswering+"\"");
        }
        
        //load seed from benchmark model
        seed = -1;
        iterator = benchmarkParamModel.listObjectsOfProperty(benchmarkParamModel.getProperty("http://w3id.org/gerbil/qa/hobbit/vocab#hasSeed"));
        if(iterator.hasNext()) {
        	try {
        		seed = iterator.next().asLiteral().getLong();
                LOGGER.info("QaBenchmark: Got seed from the parameter model: \""+seed+"\"");
            } catch (Exception e) {
                LOGGER.error("QaBenchmark: Exception while parsing parameter.", e);
            }
        }
        //check if seed is wrong sat, put the default value
        if (seed < 0) {
        	LOGGER.error("QaBenchmark: Couldn't get the seed from the parameter model. Using default value.");
        	seed = 42;
        	LOGGER.info("QaBenchmark: Setting seed to default value: \""+seed+"\"");
        }
        
        //load SparqlService from benchmark model
        sparqlService = "";
    	iterator = benchmarkParamModel.listObjectsOfProperty(benchmarkParamModel.getProperty(gerbilQaUri+"hasSparqlService"));
        if (iterator.hasNext()) {
            try {
            	sparqlService = iterator.next().asLiteral().getString();
            	LOGGER.info("QaBenchmark: Got SPARQL service from the parameter model: \""+sparqlService+"\"");
            } catch (Exception e) {
                LOGGER.error("QaBenchmark: Exception while parsing parameter.", e);
            }
        }
        //check SparqlService
        try{
		    String query = "PREFIX dbo: <http://dbpedia.org/ontology/> PREFIX dbr: <http://dbpedia.org/resource/> ask where { dbr:DBpedia dbo:license dbr:GNU_General_Public_License . }";
		    QueryExecution qexec = QueryExecutionFactory.sparqlService(sparqlService, query);
		    qexec.execAsk();
        }catch(Exception e){
        	throw this.localError("QaBenchmark: SPARQL service not accessible. Aborting.",e);
        }

        //create data generator
        LOGGER.info("QaBenchmark: Creating Data Generator "+DATA_GENERATOR_CONTAINER_IMAGE+".");
        //Setup data generator parameters
        String[] envVariables = new String[]{
        		QaDataGenerator.EXPERIMENT_TYPE_PARAMETER_KEY + "=" + experimentType.getName(),
        		QaDataGenerator.EXPERIMENT_TASK_PARAMETER_KEY + "=" + experimentTaskName,
        		QaDataGenerator.QUESTION_LANGUAGE_PARAMETER_KEY + "=" + questionLanguage,
        		QaDataGenerator.NUMBER_OF_QUESTIONS_PARAMETER_KEY + "=" + numberOfQuestionSets,
                QaDataGenerator.SEED_PARAMETER_KEY + "=" + seed,
                QaDataGenerator.SPARQL_SERVICE_PARAMETER_KEY + "=" + sparqlService,
                QaDataGenerator.DATASET_PARAMETER_KEY + "=" + experimentDataset,
                QaDataGenerator.NUMBER_OF_TRIPLES_PARAMETER_KEY +"=" + numberOfTriples
                };
        //Create data generator
        createDataGenerators(DATA_GENERATOR_CONTAINER_IMAGE, NUMBER_OF_GENERATORS, envVariables);

        //create task generator
        LOGGER.info("QaBenchmark: Creating Task Generator "+TASK_GENERATOR_CONTAINER_IMAGE+".");
        //Setup task generator parameters
        envVariables = new String[] {
        		QaTaskGenerator.EXPERIMENT_TYPE_PARAMETER_KEY + "=" + experimentType.getName(),
        		QaTaskGenerator.EXPERIMENT_TASK_PARAMETER_KEY + "=" + experimentTaskName,
        		QaTaskGenerator.QUESTION_LANGUAGE_PARAMETER_KEY + "=" + questionLanguage,
        		QaTaskGenerator.NUMBER_OF_QUESTIONS_PARAMETER_KEY + "=" + numberOfQuestionSets,
        		QaTaskGenerator.TIME_FOR_ANSWERING_PARAMETER_KEY + "=" + timeForAnswering,
				QaTaskGenerator.SEED_PARAMETER_KEY + "=" + seed,
				QaTaskGenerator.DATASET_PARAMETER_KEY + "=" + experimentDataset
				};
      //create task generator
        createTaskGenerators(TASK_GENERATOR_CONTAINER_IMAGE, NUMBER_OF_GENERATORS, envVariables);

        //create evaluation storage
        LOGGER.info("QaBenchmark: Creating Default Evaluation Storage "+DEFAULT_EVAL_STORAGE_IMAGE+".");
        createEvaluationStorage();

        //wait for all components to finish their initialization
        LOGGER.info("QaBenchmark: Waiting for components to finish their initialization.");
        waitForComponentsToInitialize();
        
        LOGGER.info("QaBenchmark: Initialized.");
    }
	
    /**
     * Executes the benchmark.
     */
	@Override
    protected void executeBenchmark() throws Exception {
		LOGGER.info("QaBenchmark: Executing benchmark.");

		//Send signals to Data generator and Task generator
        sendToCmdQueue(Commands.TASK_GENERATOR_START_SIGNAL);
        sendToCmdQueue(Commands.DATA_GENERATOR_START_SIGNAL);

        LOGGER.info("QaBenchmark: Waiting for Generators to finish.");
        
        //Wait Data generator and Task generator to finish
        waitForDataGenToFinish();
        waitForTaskGenToFinish();
        
        LOGGER.info("QaBenchmark: Waiting for System to finish.");
        if(experimentTaskName.equalsIgnoreCase(_LARGESCALE)){
        	waitForSystemToFinish(60000); //wait up to 1 more minute
        }else{
        	waitForSystemToFinish(600000); //wait up to 10 more minutes
        }
        
        LOGGER.info("QaBenchmark: Creating Evaluation Module "+EVALUATION_MODULE_CONTAINER_IMAGE+" and waiting for evaluation components to finish.");
        
        // Create evaluation model container
        createEvaluationModule(EVALUATION_MODULE_CONTAINER_IMAGE,
        		new String[] { "QAexperimentType=" + experimentType.name(),
        				"QAsparqlService="+sparqlService});
        
        // Wait evaluation model to finish
        waitForEvalComponentsToFinish();
        
        // Send the results
        LOGGER.info("QaBenchmark: Sending result model.");
        sendResultModel(this.resultModel);
        
        LOGGER.info("QaBenchmark: Benchmark executed.");
    }
	
	/**
	 * Calls super.close() Method and logs Closing-Information.
	 */
	@Override
    public void close() throws IOException {
		LOGGER.info("QaBenchmark: Closing.");
		LOGGER.info("QaBenchmark: Duration -> "+(System.currentTimeMillis() - startTime));
        super.close();
        LOGGER.info("QaBenchmark: Closed.");
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
}