#!/bin/sh

JAVAC="javac"
export JAVAC

RFA=../third-party/rfa.jar
RFA_VALUEADD=../third-party/ValueAdd_DomainRep.jar

# Apache Commons command-line-processor
COMMONS_CLI=../third-party/commons-cli-1.2.jar

# Google Core Libraries
GUAVA=../third-party/guava-17.0.jar
GSON=../third-party/gson-2.2.4.jar

# Apache Log4j2 and friends
LOG4J2=../third-party/log4j-api-2.0.jar:../third-party/log4j-core-2.0.jar
# per http://logging.apache.org/log4j/2.x/faq.html#which_jars
JAVAUTILLOGGINGAPI=../third-party/jul-to-slf4j-1.7.7.jar
SLF4JAPI=../third-party/slf4j-api-1.7.12.jar
SLF4JBINDING=../third-party/log4j-slf4j-impl-2.0.jar
LOG4J2=$LOG4J2:$SLF4JBINDING:$SLF4JAPI:$JAVAUTILLOGGINGAPI

CLASSPATH=.:$COMMONS_CLI:$GUAVA:$GSON:$LOG4J2:$RFA:$RFA_VALUEADD
export CLASSPATH

$JAVAC *.java
