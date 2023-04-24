import ftplib
import datetime
import pandas as pd
import json
from dotenv import load_dotenv
import os

load_dotenv()

pd.set_option('display.max_columns', 10)  # show all columns
pd.set_option('display.expand_frame_repr', False)  # do not wrap long lines

if "ftpuname" in os.environ:
    ftpuname = os.environ["ftpuname"]
    ftppw = os.environ["ftppw"]
else:
    # Load variables from .env file
    load_dotenv()
    ftpuname = os.getenv("ftpuname")
    ftppw = os.getenv("ftppw")


ftp = ftplib.FTP('ftp.collincountytx.gov')
ftp.login(user=ftpuname, passwd=ftppw)

files = ftp.nlst()
print(files)

ftime = datetime.datetime.now().strftime("%Y%m%d-%H-%M")
fname = 'raw/'+ftime

# Download the file
with open(fname+'.csv', 'wb') as f:
    ftp.retrbinary('RETR '+ files[0], f.write)

ftp.quit()

df = pd.read_csv(fname+'.csv')
# Create new frame and keep second row (index starts at 0) along with the header
df2 = pd.concat([df.iloc[1]], axis=1)

# Replace original dataframe, keeping the last row, but transposing the above dataframe
# Reasoning: pd.concat creates a tall, not wide, dataframe, but df[df['PRECINCT CODE']=='ZZZ'] is already wide
df = pd.concat([df2.T, df[df['PRECINCT CODE']=='ZZZ']])

# save it locally, to be safe
df.to_csv(fname+'_transposed.csv', index=False)


# start the cleanup nightmare
df = df.drop(columns=['PRECINCT CODE', 'PRECINCT NAME'])
df = pd.melt(df, id_vars=['COUNTY NUMBER', 'REGISTERED VOTERS TOTAL', 'BALLOTS CAST TOTAL', 'BALLOTS CAST BLANK'], var_name='RACE', value_name='CANDIDATES')
df.drop('REGISTERED VOTERS TOTAL', axis=1, inplace=True)


df['RACE_CANDIDATES'] = df['RACE'] + ' - ' + df['CANDIDATES']
df = df[['COUNTY NUMBER', 'RACE', 'CANDIDATES', 'RACE_CANDIDATES', 'BALLOTS CAST BLANK', 'BALLOTS CAST TOTAL']]

# df.to_csv('melted.csv', index=False)

df_new = pd.DataFrame(columns=['COUNTY NUMBER', 'RACE', 'CANDIDATES', 'VOTES', 'VOTERS', 'BALLOTS CAST TOTAL', 'BALLOTS CAST BLANK'])

datalist = []

# Some risky transformations here: we are combining every two rows together into its own little dataframe, and from there we create a dictionary
# Reasoning: the melted data has row[0] as the candidate, and row[1] as the actual vote. Could not figure out another logical way to do this

for index, row in df.iterrows():
    if index % 2 == 1:
        try:
            newdict = {}
            combined_row = pd.concat([prev_row, row], axis=0)
            
            # backup if we want to split the data out from RACE_CANDIDATES
            # print(combined_row['RACE_CANDIDATES'][0].split('-')[:1][0].split('.'))
            
            # print(combined_row)
            # print(combined_row['RACE_CANDIDATES'][0].split('-')[:1][0].split('.'))
            # print((combined_row['RACE'][0].split('.')))
            
            if len(combined_row['RACE_CANDIDATES'][0].split('-')[:1][0].split('.')[:-1]) > 2:
                newdict['RACE'] = ''.join(combined_row['RACE_CANDIDATES'][0].split('-')[:1][0].split('.')[:-1])
            else:
                item = combined_row['RACE_CANDIDATES'][0].split('-')[:1][0].split('.')

                if len(item) > 1:
                    if len(item[1].strip()) > 1 and len(item[1].strip()) < 2:
                        newdict['RACE'] = combined_row['RACE_CANDIDATES'][0].split('-')[:1][0].split('.')[0].strip()
                    elif len(item[1].strip()) > 2:
                        newdict['RACE'] = ''.join(combined_row['RACE_CANDIDATES'][0].split('-')[:1][0].split('.')[:2]).strip()
                    else:
                        newdict['RACE'] = combined_row['RACE_CANDIDATES'][0].split('-')[:1][0].split('.')[0].strip()
                else:
                    newdict['RACE'] = combined_row['RACE_CANDIDATES'][0].split('-')[:1][0].split('.')[0].strip()
            
            newdict['CANDIDATE'] = combined_row['CANDIDATES'][0]
            newdict['VOTES'] = combined_row['CANDIDATES'][1]
            newdict['COUNTY'] = combined_row['COUNTY NUMBER'][1]
            
            # print(newdict)
            datalist.append(newdict)
        
        except Exception as e:
            # print(item)
            print(combined_row)
            print(e)
    
    prev_row = row
    
# save it, and the next step is for clarity to ingest this
ndf = pd.DataFrame(datalist)
ndf.to_csv(fname+'_parsed.csv', index=False)

results = {}

groups = ndf.groupby(['RACE', 'CANDIDATE'])

for name, group in groups:
    race, candidate = name
        
    if race not in results:
        results[race] = {
            "ballots cast": "0",
            "precincts reporting": "0",
            "precincts total": "0",
            "registered voters": "0",
            "choices": {}
        }
    
    for _, row in group.iterrows():
        candidate = row["CANDIDATE"]
        if candidate not in ["OVER VOTES", "UNDER VOTES"]:
            total_votes = row["VOTES"]
            # total_pct = "{:.2f}".format(int(total_votes) / float(group["VOTES"].sum()))
            if candidate not in results[race]["choices"]:
                results[race]["choices"][candidate] = {
                    "total_votes": int(total_votes),
                    "total_pct": "0.00"
                }
            else:
                results[race]["choices"][candidate]["total_votes"] += int(total_votes)
                results[race]["choices"][candidate]["total_pct"] = "0.00"

for race, race_data in results.items():
    choices = race_data['choices']
    total_votes = sum(c_data['total_votes'] for c_data in choices.values())
    for choice_data in choices.values():
        choice_data['total_pct'] = '{:.2f}'.format(choice_data['total_votes'] / total_votes) if total_votes > 0 else '0.00'


result_string = json.dumps(results)

with open("data/"+ftime+".json", "w") as json_file:
    json_file.write(result_string)
    
with open("data/latest.json", "w") as json_file:
    json_file.write(result_string)