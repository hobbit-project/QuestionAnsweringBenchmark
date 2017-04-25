default: build dockerize 

build:
	mvn clean package -U -Dmaven.test.skip=true 

dockerize: 	
	docker build -f qa_benchmark_controller.docker -t git.project-hobbit.eu:4567/cmartens/qabenchmarkcontroller .
	docker build -f qa_data_generator.docker -t git.project-hobbit.eu:4567/cmartens/qadatagenerator . 
	docker build -f qa_task_generator.docker -t git.project-hobbit.eu:4567/cmartens/qataskgenerator .
	
dockerizeBm: 	
	docker build -f qa_benchmark_controller.docker -t git.project-hobbit.eu:4567/cmartens/qabenchmarkcontroller .
	
dockerizeDg: 
	docker build -f qa_data_generator.docker -t git.project-hobbit.eu:4567/cmartens/qadatagenerator .
	
dockerizeTg: 
	docker build -f qa_task_generator.docker -t git.project-hobbit.eu:4567/cmartens/qataskgenerator .
	
push: 	
	docker push git.project-hobbit.eu:4567/cmartens/qabenchmarkcontroller
	docker push git.project-hobbit.eu:4567/cmartens/qadatagenerator 
	docker push git.project-hobbit.eu:4567/cmartens/qataskgenerator