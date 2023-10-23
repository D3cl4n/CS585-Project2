import pandas as pd
from pandasql import sqldf

pysqldf = lambda q: sqldf(q, globals())
facein=pd.read_csv("FaceIn.csv")
associates=pd.read_csv("associates.csv")
access=pd.read_csv("access.csv")

sqldf('''select distinct f.ID,f.name, cnt as numOfRel,avg as average from facein f 
inner join 
(select personA_ID as personA_ID, count(*) as cnt from associates group by personA_ID
union select personB_ID, count(*) as cnt from associates group by personB_ID ) ass on ass.personA_ID=f.ID
cross join (select allcount, count(distinct personA_ID), allcount/count(distinct personA_ID) as avg from 
(select personA_ID from associates union select personB_ID from associates ) a
cross join 
(select 'all' , count(*) as allcount from facein ) all1 ) avg 
where cnt>avg
order by cnt desc
 ''')
