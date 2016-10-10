#!/bin/bash

DOCKER_IMAGE='clux/muslrust'
CONTAINER_USERNAME='dummy'
CONTAINER_GROUPNAME='dummy'
HOMEDIR='/home/'$CONTAINER_USERNAME
GROUP_ID=$(id -g)
USER_ID=$(id -u)

### FUNCTIONS

create_user_cmd()
{
  echo \
    groupadd -f -g $GROUP_ID $CONTAINER_GROUPNAME '&&' \
    useradd -u $USER_ID -g $CONTAINER_GROUPNAME $CONTAINER_USERNAME '&&' \
    chown -R $CONTAINER_USERNAME:$CONTAINER_GROUPNAME $HOMEDIR '&&' \
    cd $HOMEDIR 
}
    
#mkdir --parent $HOMEDIR '&&' \

execute_as_cmd()
{
  echo \
    su $CONTAINER_USERNAME -c 
}

full_container_cmd()
{
  echo "'$(create_user_cmd) && $(execute_as_cmd) \"$@\"'"
}

### MAIN

eval sudo docker run -it \
    --rm=true \
    -a stdout \
    --privileged=true \
    -v $(pwd):$HOMEDIR \
    $DOCKER_IMAGE \
    /bin/bash -ci $(full_container_cmd "$@")
