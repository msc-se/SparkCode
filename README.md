# SparkCode
This repository consists of the two different Spark jobs: The streaming preprocessing Process-tweets and the batch processing Processing-covid

# Compile

Each project can compiled to a jar with the following command in each project:

```
mvn package -f "pom.xml"
```

This will output a file called `<artifactid><verison>-jar-with-dependencies.jar`

# Location
The compiled jar files is located under the *~/scripts/bin/* folder on the Node-master, and can be executed by the sh files in the *~/scripts/* folder.
