#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import cherrypy

import os
os.getcwd()
#os.chdir("C:\\Users\\vikas\\zerodha_task")
#os.getcwd()

url_ = 'https://www.bseindia.com/markets/MarketInfo/BhavCopy.aspx'


def download_extract_zip():
    '''
    Fetches the URL where the zip file is located
    Also it downloads the zip and stores the file downloaded
    '''
    url = urlopen(url_)
    html = url.read()
    soup = BS(html, "html.parser")
    equity_file = soup.find(id='ContentPlaceHolder1_btnhylZip')
    link = equity_file.get('href', None)
    r = requests.get(link) #response 200 
    zipfile_csv = zipfile.ZipFile(io.BytesIO(r.content))
    zipfile_csv.extractall()
    return zipfile_csv.namelist()[0]




def save_data(data_file):
    '''
    Stores the result of the CSV in Redis
    '''
    csv_data = pd.read_csv(data_file)
    # The required fileds are stored
    csv_data = csv_data[['SC_CODE', 'SC_NAME', 'OPEN', 'HIGH', 'LOW', 'CLOSE']].copy()
    for index, row in csv_data.iterrows():
        r.hmset(row['SC_CODE'], row.to_dict())
        r.set("equity:"+row['SC_NAME'], row['SC_CODE'])


class Bhav_page:
    @cherrypy.expose
    def index(self, search=""):
        '''
        Fetches the top 10 result from the redis and
        renders it to the frontend
        '''
        env = jinja2.Environment(loader=jinja2.FileSystemLoader(os.path.join('template')))
        template_file = env.get_template('index.html')
        self.outdata = []
        for key in r.scan_iter("equity:*"):
            code = r.get(key)
            self.outdata.append(r.hgetall(code).copy())
            self.outdata = self.outdata[0:10]
        return template_file.render(outdata=self.outdata)
    
    
import redis
from urllib.request import urlopen
from bs4 import BeautifulSoup as BS
import requests
import zipfile
import io
import pandas as pd
import os
import jinja2
from jinja2 import Environment, FileSystemLoader

if __name__ == '__main__':
    data_file = download_extract_zip()
    r = redis.StrictRedis(host="127.0.0.1",
        port=6379,
        charset="utf-8",
        decode_responses=True,
        db=1)
    save_data(data_file)
    
    
    config = {'global': {'server.socket_host':  '0.0.0.0',
                'server.socket_port':  int(os.environ.get('PORT', 8085))}}
    cherrypy.quickstart(Bhav_page(),config=config)
