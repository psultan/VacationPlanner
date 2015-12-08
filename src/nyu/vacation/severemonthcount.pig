/*count the severe storms per month*/

REGISTER /usr/jars/piggybank.jar;
data = LOAD '/media/sf_Desktop/VacationPlanner/sandbox/paul/GoogleMapper/resources/severedata/ftp/Storm*' USING org.apache.pig.piggybank.storage.CSVExcelStorage() AS (a,b,c,d,e,f,g,h,i,j:int,k,l,m,n,o:int,p,q,r,s,t,u,v,w,x,y,z,aa,ab,ac,ad,ae,af,ag,ah,ai,aj,ak,al,am,an,ao,ap,aq,ar,ass,at,au,av,aw,ax,ay);
data = FILTER data BY $0 != 'BEGIN_YEAR';
data = FILTER data BY $0 != 'BEGIN_YEARMONTH';
months = FOREACH data GENERATE SUBSTRING($0,4,6);
monthgroup = GROUP months by $0;
count = FOREACH monthgroup GENERATE $0,COUNT($1);
dump count;

