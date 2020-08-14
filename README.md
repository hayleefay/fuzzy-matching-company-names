# Fuzzy Matching
This repo has code to fuzzy match other data sources with company names to companies found in the Burning Glass dataset. This is done by using map reduce and blocking on first letter of cleaned (removing articles and expanding common abbreviations) company names.

Company names are matched using string distance measures (using the fuzzy wuzzy package) between two companies' cleaned names, then their more radically cleaned names (removing very common words found in the corpus) and then their company names with all English words removed. If a match has above a threshold score for at least one of these tests, then it will be included as a possible match. A higher threshold of scores will be chosen heuristically for the final matches.

### Steps to fuzzy match:
1. Pull the unique employer names from Burning Glass by running the following sql query: `\copy(SELECT DISTINCT(employer) FROM maintext) to '{path}/unique_employer.csv' CSV HEADER;`
2. Run the fuzzy matching: `python fuzzy_matching.py {external employers_of_interest file} --employer_file=unique_employer.csv --english_words=words_alpha.txt > output.txt`
3. Create a CSV of the matches: `python create_csv.py`
4. View the matches and decide if you would like to raise/lower/change the threshold of the matching algorithm and/or manually remove matches.
5. If you alter the threshold, rerun step 2-4.
6. Once satisfied with the matches, run the scripts in the `create_sql_queries` folder in order to create the queries needed to pull the data from Burning Glass with the matched company names.
    - `python create_sql_queries/create_query_maintext.py`
    - `python create_sql_queries/create_query_skills.py`
    - `python create_sql_queries/create_query_jobtext.py`
7. Run these sql queries against the Burning Glass database in order to pull job postings (including skills, text, and structured data) that was posted by a fuzzy matched employer.

																																																																			


