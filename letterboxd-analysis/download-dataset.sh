#!/bin/bash

curl -L -o $PWD/letterboxd.zip\
  https://www.kaggle.com/api/v1/datasets/download/gsimonx37/letterboxd

unzip -j letterboxd.zip "archive/*" -d data