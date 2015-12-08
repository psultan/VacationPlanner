/*test normalizing intermediate values*/
/*author: paul*/

A = LOAD '/media/sf_Desktop/VacationPlanner/sandbox/paul/GoogleMapper/resources/severedata/ftp/results/part-r-00000' USING PigStorage() as (day:double, total:double);
B = ORDER A BY total;
C = ORDER A BY total DESC;
min = LIMIT B 1;
max = LIMIT C 1;
scaled = FOREACH A GENERATE $0, (10-1)/(max.total-min.total)*($1-max.total)+10;
scaled = ORDER scaled BY $1;
dump scaled;


