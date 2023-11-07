#!/bin/sh
cd /datamart/code1.3/
python3 Bihar_pfm_datamart_report-weekly.py
python3 Bihar_pfm_datamart_report-periodic.py
cd /datamart/BiharDatamart/
git pull origin master
git add --all
git commit -m "Datamart Report"
git push origin master
