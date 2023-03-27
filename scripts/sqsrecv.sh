#!/bin/sh

java -cp `pwd`/build/libs/aws-patterns-simpleapi-allinone.jar com.luxant.examples.SqsReceive $*
