
#!/bin/bash
# set debug mode
set -x

# output log of userdata to /var/log/user-data.log
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1
# create and put some content in the file
touch /home/ec2-user/created_by_userdata.txt
(
cat << 'EOP'
Hey there!!!
EOP
) > /home/ec2-user/created_by_userdata.txt
yes | easy_install pip
yum install -y gcc
yum install -y python-devel
yes | pip install Twisted==16.4.1
yes | pip install scrapy
yes | pip install bs4
yes | pip install boto3
yes | pip install --upgrade --user awscli
export PATH=~/.local/bin:$PATH
source ~/.bash_profile
aws s3 sync s3://crawlerslaunchbucket crawler
python crawler/Cloud_Crawler/NewsCrawler.py