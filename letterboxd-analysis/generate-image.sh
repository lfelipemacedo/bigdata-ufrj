#!/bin/bash

mvn clean package
docker build -t letterboxd-analysis .