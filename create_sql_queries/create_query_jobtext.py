# This file will create the query that pulls all of the job postings that have an
# employer that was matched from other data source
import pandas as pd
filepath_root = os.getcwd()

df = pd.read_csv(filepath_root + '/data/maintext_with_employer.csv')
ids = df['bgtjobid'].unique().to_list()
ids = ids[:50000]

query_start = "\copy (SELECT * FROM jobtext WHERE bgtjobid IN ("
query_end = f")) to '{filepath_root}/data/jobtext_1.csv' csv header;"

query_middle = ""
for i in ids:
    addition = "%s" % i
    addition += ", "

    query_middle += addition

# remove final comma
query_middle = query_middle[:-2]

query = query_start + query_middle + query_end

with open('pull_jobtext_1.sql', 'w') as f:
    f.write(query)
