# Predera Hive JDBC Lib

Predera's Hive JDBC stage lib is built on top of Streamsets JDBC stage lib, so apart from the other databases supported by SDC JDBC lib using Predera's Hive JDBC lib users can interact with Hive also.

As a part of this project, our Hive JDBC lib supports the following Hive Origin, Destination, Executor and Processor

## Getting Started

These instructions will guide you through how to build our Predera?s Hive JDBC stage lib and integrate with streamsets data collector on the fly.

## Installing


We have cloned the Streamsets JDBC lib built and made our changes on top of it to support Hive

Before building Predera Hive JDBC Stage lib, you will have to clone Streamsets data collector api and other dependencies

So you might find it easiest to download tarballs from the relevant GitHub release pages rather than using git clone:

    https://github.com/streamsets/datacollector/releases
    https://github.com/streamsets/datacollector-api/releases`

_Note : Use SDC 2.7 or later_

You will need to build both the Data Collector and its API. Since we just need the pipeline library JAR files and we already have the SDC runtime, we can skip building the GUI and running tests, saving a bit of time:

    $ cd datacollector-api
    $ mvn clean install -DskipTests
    ...output omitted...
    $ cd ../datacollector
    $ mvn clean install -DskipTests
    ...output omitted...

Maven puts the library JARs in its repository, so they?re available when we build our custom processor:

    $ ls ~/.m2/repository/com/streamsets/streamsets-datacollector-commonlib/2.1.0.0/
    _remote.repositories
    streamsets-datacollector-commonlib-2.1.0.0-tests.jar
    streamsets-datacollector-commonlib-2.1.0.0.jar
    Streamsets-datacollector-commonlib-2.1.0.0.pom
  
### Predera Hive JDBC stage lib
  
Now clone or download the Predera's Hive JDBC stage lib from [here](https://github.com/predera/hivejdbclib).

Then build the stage lib using maven as

    mvn clean package -DskipTests   

After a successful build, you can see the build template as

    [INFO] Building tar: /home/kiran/git_projects/hive-jdbc-stage-lib/target/Predera-Hive-JDBC-lib-1.0-SNAPSHOT.tar.gz
    [INFO] ------------------------------------------------------------------------
    [INFO] BUILD SUCCESS
    [INFO] ------------------------------------------------------------------------
    [INFO] Total time: 02:10 min
    [INFO] ------------------------------------------------------------------------

## Deployment

Extract the tarball to the SDC user-libs directory, restart SDC, and you should see the sample stages in the stage library:

    $ cd ~/streamsets-datacollector-2.7.0.0/user-libs/
    $ tar xvfz /home/kiran/git_projects/hive-jdbc-stage-lib/target/Predera-Hive-JDBC-lib-1.0-SNAPSHOT.tar.gz .

After restarting SDC, you should be able to see Predera's Hive JDBC components. 

## Built With

Maven - Dependency Management

## Contributing

Please read [CONTRIBUTING.md](https://gitlab.com/predera/platform/hive-jdbc-stage-lib/contributing.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Authors

Kiran Krishna Innamuri - Initial work - [Predera Technologies](http://www.predera.com)

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE.md](https://gitlab.com/predera/platform/hive-jdbc-stage-lib/License.txt) file for details

