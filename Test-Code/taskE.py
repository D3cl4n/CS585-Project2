# code was a part of jupiter notebook
import pandas as pd
from pandasql import sqldf

pysqldf = lambda q: sqldf(q, globals())
facein=pd.read_csv("FaceIn.csv")
associates=pd.read_csv("associates.csv")
access=pd.read_csv("access.csv")

taskE=sqldf('''select f.ID,name, count(WhatPage) countaccess,count(distinct WhatPage)  cntdistaccess from facein f 
inner join access on access.ByWho=f.ID
 group by f.name,f.ID  order by count(WhatPage) desc''')
taskE
#.to_csv('taskE.csv')
