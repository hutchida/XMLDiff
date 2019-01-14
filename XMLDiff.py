import sys
import pandas as pd
import numpy as np
import time
import timeit
import httplib2
import requests
from bs4 import BeautifulSoup, SoupStrainer
import csv
import os
import glob
import datetime
import re
import string


if sys.version_info[0] < 3:
    from StringIO import StringIO
else:
    from io import StringIO
    

date =  str(time.strftime("%d/%m/%Y"))
css = """<head>
            <style>
            th {
                background-color: #69ab96;
                color: f5f5f0;
            }
    
            tr:nth-child(even) {background-color: #f2f2f2;} 
    
            table, th, td, p {
               border: 0px solid black;
               font-family: Calibri;
               text-align: left;
               padding: 5px;
               font-size: 14px;
               border-spacing: 5px;
               vertical-align: top;
            }
            
            a {
                font-family: Calibri;
            }
            tr:hover {background-color: #F8E0F7;}

            h1, h2, h3, h4, h5, h6 {
	font-family: Calibri;
}
    
            </style>
            </head>"""

logdir = 'logs//'
xmldir = 'xml//'
filename1 = '2018_restructuring_&_insolvency.xml'
filename2 = '2019_restructuring_&_insolvency.xml'
log1 = '2018_restructuring_&_insolvency.csv'
log2 = '2019_restructuring_&_insolvency.csv'

def CSV(logdir, xmldir, filename):
    print("Scanning documents and extracting key information...")
    df = pd.DataFrame()
    file = os.path.join(xmldir, filename)    
    soup = BeautifulSoup(open(file),'lxml-xml') 
    jurisdictions = soup.jurisdictions
    for jurisdiction in jurisdictions.findAll('jurisdiction'):
        j = jurisdiction['name']
        for question in jurisdiction.findAll('question'):
            number = question.number.text
            title = question.title.text
            q = question.full.text
            answer = question.answer.text
            answer = answer.replace('<p>','').replace('</p>','')
            
            key = j + str(number)
            if title == 'Updates and trends':
                key = j + str(number) + title

            list1 = [[filename, j, number, title, q, answer, key]]           
            df = df.append(list1)

    filename = re.search('([^\.]*)\.xml', filename).group(1)
    logpath = logdir + filename + '.csv'
    df.to_csv(logpath, sep=',',index=False, header=["Filename", "Jurisdiction", "Number", "Title", "Question", "Answer", "Key"], encoding="UTF8")



def Compare(logdir, logold, lognew):
    print("Opening two log files now...")
    df1 = pd.read_csv(logdir + logold, encoding ='utf-8')
    df2 = pd.read_csv(logdir + lognew, encoding ='utf-8')

    print("df1 number of rows before cull: " + str(len(df1)))
    print("df2 number of rows before cull: " + str(len(df2)))


    print("Discovering additions and deletions...")
    #find new
    df_new = pd.merge(df1,df2, on='Key', how='outer', indicator=True)
    print("Extracting new paras only...")
    df_new = df_new.ix[df_new['_merge']=='right_only']
    #delete newly generated columns from the merge function that we don't need
    del df_new['Filename_x']
    del df_new['Jurisdiction_x']
    del df_new['Number_x']
    del df_new['Title_x']
    del df_new['Question_x']
    del df_new['Answer_x']
    del df_new['_merge']
    df_new = df_new.rename(columns={'Filename_y': 'Filename', 'Jurisdiction_y': 'Jurisdiction', 'Number_y': 'Number', 'Title_y': 'Title', 'Question_y': 'Question', 'Answer_y': 'Answer'})

    new_len = str(len(df_new.index))
    print("Removing new rows from the latest dataframe in order to do a valid comparison...")

    for x in range(len(df_new.index)):
        key = df_new['Key'].iloc[x]
        #print(key)
        df2 = df2[df2['Key'] != key]
        #df1 = df1[df1['Key'] != key]

    #removing superfluous columns for final presentation
    #del df_new['Filename']
    del df_new['Key']

    #resetting the index after row removal
    df2 = df2.reset_index(drop=True)
    #df1 = df1.reset_index(drop=True)


    #find deleted paras
    df_del = pd.merge(df2,df1, on='Key', how='outer', indicator=True)

    print("Extracting deleted paras only...")
    df_del = df_del.ix[df_del['_merge']=='right_only']
    #delete newly generated columns from the merge function that we don't need
    del df_del['Filename_x']
    del df_del['Jurisdiction_x']
    del df_del['Number_x']
    del df_del['Title_x']
    del df_del['Question_x']
    del df_del['Answer_x']
    del df_del['_merge']
    df_del = df_del.rename(columns={'Filename_y': 'Filename', 'Jurisdiction_y': 'Jurisdiction', 'Number_y': 'Number', 'Title_y': 'Title', 'Question_y': 'Question', 'Answer_y': 'Answer'})

    del_len = str(len(df_del.index))
    print("Removing deleted rows from the previous dataframe in order to do a valid comparison...")

    for x in range(len(df_del.index)):
        pattern = df_del['Key'].iloc[x]
        #print(key)
        df1 = df1[df1['Key'] != pattern]

    #removing superfluous columns for final presentation
    del df_del['Key']

    #resetting the index after row removal
    df1 = df1.reset_index(drop=True)

    #exporting to csv to see what's happening
    df1.to_csv(logdir + 'df1.csv', sep=',',index=False,header=["Filename", "Jurisdiction", "Number", "Title", "Question", "Answer", "Key"])
    df2.to_csv(logdir + 'df2.csv', sep=',',index=False,header=["Filename", "Jurisdiction", "Number", "Title", "Question", "Answer", "Key"])

    print("df1 number of rows after cull: " + str(len(df1)))
    print("df2 number of rows after cull: " + str(len(df2)))

    #COMPARISON
    print("Merging paras that both logs share...")
    df = pd.concat([df1, df2], axis='columns', keys=['Previous', 'Current'])

    #swap columns around so they're next to eachother, easier to spot the difference
    df_final = df.swaplevel(axis='columns')[df1.columns[0:]]

    #define the highlighting function
    def highlight_diff(data, color='yellow'):
        attr = 'background-color: {}'.format(color)
        other = data.xs('Previous', axis='columns', level=-1)
        return pd.DataFrame(np.where(data.ne(other, level=0), attr, ''),
                            index=data.index, columns=data.columns)

    #compare previous and current and mark in new column whether any change
    def f(x):
        state = "no"
        
        #test for question change
        prev = str(x['Question','Previous'])
        curr = str(x['Question','Current'])
        if  prev and curr != 'nan':
                if prev != curr:
                    state = 'yes'
                else:
                    state = 'no'
        else:
                state = 'no'   

        if state == 'no':
                #test for answer change
                prev = str(x['Answer','Previous'])
                curr = str(x['Answer','Current'])
                if  prev and curr != 'nan':
                    if prev != curr:
                        state =  'yes'
                    else:
                        state =  'no'
                else:
                    state =  'no'

        if state == 'no': return 'no' 
        else: return 'yes'


    print("Finding differences between logs...")
    #adding a new column for changes
    df_final['Changed'] = df_final.apply(f, axis=1)
    # Filter the data by new column that indicates changes
    df_final = df_final[df_final['Changed'] == 'yes']
    change_len = str(len(df_final.index))
    print("Number of changes found: " + change_len)

    #Exporting useful dataframes to csv
    print("Exporting useful dataframes to csv...")
    filename = re.search('([^\.]*)\.csv', lognew).group(1)
    df_new.to_csv(logdir + filename + "_additions.csv", sep=',',index=False)
    df_del.to_csv(logdir + filename + "_deletions.csv", sep=',',index=False)
    df_final.to_csv(logdir + filename + "_changes.csv", sep=',',index=False)
    link = '<a href="file:///' + logdir + '">' + logdir + '</a>'


    print("Exporting to html format...")
    #build html
    html = r'<link rel="stylesheet" type="text/css" media="screen" />'+ '\n' + css
    html += '<h1 id="home">GTDT UPDATES OVERVIEW FOR: ' + filename + '</h1>' 
    html += r'<p><b>' + new_len + '</b> new paras have been added, <a href="#new">jump to</a></p>' 
    html += r'<p><b>' + del_len + '</b> paras have been deleted, <a href="#del">jump to</a></p>'
    html += r'<p><b>' + change_len + '</b> changes have been found, <a href="#change">jump to</a></p>'
    html += r'<h1 id="new">ADDITIONS</h1><p>' + new_len + ' new paras have been added recently and are displayed below, (<a href="#home">scroll to top</a>)</p>'
    html += df_new.style.render()
    html += r'<h1 id="del">DELETIONS</h1><p>' + del_len + ' paras have been deleted recently and are displayed below, (<a href="#home">scroll to top</a>)</p>'
    html += df_del.style.render()
    html += r'<h1 id="change">CHANGES</h1><p>' + change_len + ' changes have been made recently and are displayed below, (<a href="#home">scroll to top</a>)</p>'
    html += df_final.style.apply(highlight_diff, axis=None).render()

    print("Writing results to HTML file...")
    with open(logdir + filename + "_updates_overview.html",'w', encoding='UTF8') as f:
        f.write(html)
        f.close()
        pass

CSV(logdir, xmldir, filename1)
CSV(logdir, xmldir, filename2)
Compare(logdir, log1, log2)
