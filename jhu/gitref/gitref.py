import pandas as pd
#set issue yyyy-mm-dd
issue = "2022-12-13"

gitref = pd.read_csv("gitrefs.csv", index_col=[0])
ref_issue = gitref.loc[gitref["issue_date"] == issue]

dateform = issue.replace("-","")
ref_issue.to_csv(f"gitref_{dateform}.csv", index=None)