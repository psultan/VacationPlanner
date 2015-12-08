/*
find all events for a state-county
*/

REGISTER /usr/jars/piggybank.jar;

data = LOAD '/media/sf_Desktop/VacationPlanner/sandbox/paul/GoogleMapper/resources/severedata/ftp/Storm*' USING org.apache.pig.piggybank.storage.CSVExcelStorage() AS (a,b,c,d,e,f,g,h,i,j:int,k,l,m,n,o:int,p,q,r,s,t,u,v,w,x,y,z,aa,ab,ac,ad,ae,af,ag,ah,ai,aj,ak,al,am,an,ao,ap,aq,ar,ass,at,au,av,aw,ax,ay);
penalty = LOAD '/media/sf_Desktop/VacationPlanner/sandbox/paul/GoogleMapper/penalty' USING PigStorage(',') AS (event:chararray, pen:int);
J = JOIN data BY m, penalty BY event;
F = FILTER J BY data::j==6 and data::o==75;
STORE F INTO '/media/sf_Desktop/VacationPlanner/sandbox/paul/GoogleMapper/severecheck' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE');

