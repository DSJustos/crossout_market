# -*- coding: utf-8 -*-

import requests
import pandas as pd
from bs4 import BeautifulSoup

# download wikipage
url = "https://crossoutdb.com/"  # "https://crossoutdb.com/#preset=crafting.craftable=true.order=20desc."
result = requests.get(url)

# if successful parse the download into a BeautifulSoup object, which allows easy manipulation 
if result.status_code == 200:
    soup = BeautifulSoup(result.content, "html.parser")
else:
    print("ERROR: parser was unable to access the provided url")
    
# find the object with HTML class wikitable sortable
table = soup.find('table',{'id':'ItemTable2'})

# loop through all the rows and pull the text
new_table = []
for row in table.find_all('tr')[1:]:
    column_marker = 0
    columns = row.find_all('td')
    new_table.append([column.get_text() for column in columns])
    
df = pd.DataFrame(new_table, columns=['ContinentCode','Alpha2','Alpha3','PhoneCode','Name'])
#df['Name'] = df['Name'].str.replace('\n','')
df