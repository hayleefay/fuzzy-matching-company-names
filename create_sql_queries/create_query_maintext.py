# This file will create the query that pulls all of the job postings that have an
# employer that was matched from from other data source
import pandas as pd
filepath_root = os.getcwd()

companies_df = pd.read_csv("matches.csv")
companies = companies_df['employer'].unique().to_list()

query_start = "\copy (SELECT * FROM maintext_with_employer where "
query_end = f") to '{filepath_root}/data/maintext_with_employer.csv' csv header;"

query_middle = ""
for company in companies:
    # check if has a single quote and replace with double
    if "'" in company:
        parts = company.split("'")
        company = "''".join(parts)
    addition = "employer = '"
    addition += company
    addition += "' or "

    query_middle += addition

# remove the final or
query_middle = query_middle[:-4]

query = query_start + query_middle + query_end

with open('pull_maintext.sql', 'w') as f:
    f.write(query)
