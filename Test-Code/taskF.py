import pandas as pd
from pandasql import sqldf

pysqldf = lambda q: sqldf(q, globals())
facein=pd.read_csv("FaceIn.csv")
associates=pd.read_csv("associates.csv")
access=pd.read_csv("access.csv")

taskF=sqldf('''select distinct ID,Name from facein f inner join associates a on f.ID=a.PersonA_ID
left join access on access.ByWho=f.ID
where access.ByWho is null''')
taskF
