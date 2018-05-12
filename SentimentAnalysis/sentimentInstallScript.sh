#!/bin/bash
sudo pip install textblob
sudo pip install boto3
sudo python -m textblob.download_corpora
sudo aws s3 sync s3://sentimentlaunchbucket .
python downloadNltk.py