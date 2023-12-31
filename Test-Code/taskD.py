import pandas as pd
from pandasql import sqldf

pysqldf = lambda q: sqldf(q, globals())
facein=pd.read_csv("FaceIn.csv")
associates=pd.read_csv("associates.csv")
access=pd.read_csv("access.csv")


sqldf('''select f.Name, ifnull(count(PersonA_ID),0) as happinessFactor from 
facein f left join associates a on f.ID=a.PersonA_ID group by Name ''')
