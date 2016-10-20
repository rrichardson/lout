
.PHONY:	build push compile

TAG = 0.3

all: push

compile:
	./run_in_docker.sh cargo build --release

build: compile
	sudo docker build -t git.permissiondata.com:4567/devops/lout:$(TAG) .

push: build
	sudo docker push git.permissiondata.com:4567/devops/lout:$(TAG)
