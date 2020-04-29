#!/usr/bin/env bash

cd targetSite
for f in *.csv
do
	mysql -uroot --password=$1 -e "USE webrankingormv5beNEW1; LOAD DATA LOCAL INFILE '"$f"'INTO TABLE target_site fields TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES"
done

cd ..

cd dailyVisitRank
for f in *.csv
do
	mysql -uroot --password=$1 -e "USE webrankingormv5beNEW1; LOAD DATA LOCAL INFILE '"$f"'INTO TABLE daily_visit_rank fields TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 LINES (dailySessionCount,dailySessionDuration,dailyVisitCount,dailyVisitorCount,date,rankScore,targetSiteID,logTypeID,rankNum)"
done
