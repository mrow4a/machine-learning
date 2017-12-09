#!/bin/bash

#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

cd $DIR && \
docker run -ti --rm -v $(pwd):/code -v "$HOME/.m2":/root/.m2 iflavoursbv/mvn-sbt-openjdk-8-alpine:latest mvn clean install