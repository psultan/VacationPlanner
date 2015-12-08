/*
aggregates severe weather per county
author: paul
*/

REGISTER /usr/jars/piggybank.jar;
REGISTER /home/cloudera/Desktop/pigleft/leftformat.jar;

data = LOAD '/media/sf_Desktop/VacationPlanner/sandbox/paul/GoogleMapper/resources/severedata/ftp/Storm*' USING org.apache.pig.piggybank.storage.CSVExcelStorage() AS (a,b,c,d,e,f,g,h,i,j:int,k,l,m,n,o:int,p,q,r,s,t,u,v,w,x,y,z,aa,ab,ac,ad,ae,af,ag,ah,ai,aj,ak,al,am,an,ao,ap,aq,ar,ass,at,au,av,aw,ax,ay);
--data = LOAD '/media/sf_Desktop/VacationPlanner/sandbox/paul/GoogleMapper/resources/severedata/sample/single.csv' USING org.apache.pig.piggybank.storage.CSVExcelStorage() AS (a,b,c,d,e,f,g,h,i,j:int,k,l,m,n,o:int,p,q,r,s,t,u,v,w,x,y,z,aa,ab,ac,ad,ae,af,ag,ah,ai,aj,ak,al,am,an,ao,ap,aq,ar,ass,at,au,av,aw,ax,ay);
penalty = LOAD '/media/sf_Desktop/VacationPlanner/sandbox/paul/GoogleMapper/penalty' USING PigStorage(',') AS (event:chararray, pen:int);
realfips = LOAD '/media/sf_Desktop/VacationPlanner/sandbox/paul/GoogleMapper/realfips' USING PigStorage(' ') AS (realfip:chararray);

J = JOIN data BY m, penalty BY event;
--01-56 3=American Samoa, 7=Canal Zone, 14=Guam, 43=Puerto Rico, 52=Virgin Islands
J = FILTER J BY data::j<=56 AND data::j!=3 AND data::j!=7 AND data::j!=14 AND data::j!=43 AND data::j!=52 AND data::j>0;
clean = FOREACH J GENERATE CONCAT((chararray)data::j,format.LEFTPAD(3,(chararray)data::o)) AS FIPS, data::m AS event, penalty::pen AS score;
G = GROUP clean BY FIPS;
total = FOREACH G GENERATE group AS tFIPS, SUM(clean.score) AS tsum;
realtotal = JOIN realfips BY realfip, total BY tFIPS;
cleantotal = FOREACH realtotal GENERATE CONCAT(realfips::realfip,'.0'), total::tsum;
ord = ORDER cleantotal BY $1;
r = RANK ord BY total::tsum;
STORE r INTO '/media/sf_Desktop/VacationPlanner/sandbox/paul/GoogleMapper/severetally' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE');
