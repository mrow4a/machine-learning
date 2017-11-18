DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

docker run -ti --rm -v $DIR:/code -v "$HOME/.ivy2":/root/.ivy2 iflavoursbv/mvn-sbt-openjdk-8-alpine:latest sbt clean package


