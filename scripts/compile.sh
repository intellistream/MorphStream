#!/bin/bash

cd ..
mvn clean package
cp application/target/application-0.0.2-jar-with-dependencies.jar scripts/
cd scripts