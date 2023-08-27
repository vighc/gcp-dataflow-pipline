#!pip install google-cloud-pubsub

# Import libraries
import os
import json
import requests
import pandas as pd
from bs4 import BeautifulSoup
from google.cloud import pubsub_v1

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] ='/content/sample_data/key.json'

def push_pubsub(message):
    publisher = pubsub_v1.PublisherClient()
    topic_id = 'projects/project_id/topics/dataflow-pubsub'

    data = message.encode('utf-8')
    future = publisher.publish(topic_id, data)
    print(f"Published message: {message}")
    future.result()

def data_extract():
    # Create an URL object
    url = 'https://www.worldometers.info/coronavirus/'
    # Create object page
    page = requests.get(url)

    # parser-lxml = Change html to Python friendly format
    # Obtain page's information
    soup = BeautifulSoup(page.text, 'lxml')

    # Obtain information from tag <table>
    table1 = soup.find('table', id='main_table_countries_today')

    # Obtain every title of columns with tag <th>
    headers = []
    for i in table1.find_all('th'):
        title = i.text
        headers.append(title)
    headers[13] = 'Tests/1M pop'

    #create dataframe
    mydata = pd.DataFrame(columns = headers)

    # Create a for loop to fill mydata
    for j in table1.find_all('tr')[1:]:
        row_data = j.find_all('td')
        row = [i.text for i in row_data]
        length = len(mydata)
        mydata.loc[length] = row

    #clean data
    mydata.drop(mydata.index[0:7], inplace=True)
    mydata.drop(mydata.index[222:229], inplace=True)
    mydata.reset_index(inplace=True, drop=True)
    mydata.drop('#', inplace=True, axis=1)

    mydata.columns.values[::] =['Country','TotalCases','NewCases','TotalDeaths','NewDeaths','TotalRecovered','NewRecovered','ActiveCases','Serious_Critical','TotalCases 1M','Deaths','TotalTests','Tests','Population','Continent','1 Caseevery X ppl','1 Deathevery X ppl','1 Testevery X ppl','New Cases','New Deaths','Active Cases']
    #push data
    for index, row in mydata.iterrows():
        data=row.to_dict()
        data=json.dumps(data)
        push_pubsub(data)
 
if __name__ == '__main__':
    data_extract()
