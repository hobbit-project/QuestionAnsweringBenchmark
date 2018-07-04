default: build dockerize 

build:
	mvn clean package -U -Dmaven.test.skip=true 

dockerize: 	
	docker build -f qa_benchmark_controller.docker -t git.project-hobbit.eu:4567/weekmo/qacontrollerv3 .
	docker build -f qa_data_generator.docker -t git.project-hobbit.eu:4567/weekmo/qadatagenv3 .
	docker build -f qa_task_generator.docker -t git.project-hobbit.eu:4567/weekmo/qataskgenv3 .
	
push: 	
	docker push git.project-hobbit.eu:4567/weekmo/qacontrollerv3
	docker push git.project-hobbit.eu:4567/weekmo/qadatagenv3
	docker push git.project-hobbit.eu:4567/weekmo/qataskgenv3