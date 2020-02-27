# -*- coding: utf-8 -*-
"""
Created on Thu Feb 20 11:22:12 2020

@author: FL014036

"SharePointAdmin@cocacolaflorida.com", "P@$$word"
"""

from shareplum import Site
from shareplum import Office365
import pandas as pd


authcookie = Office365('https://cocacolaflorida.sharepoint.com', username='SharePointAdmin@cocacolaflorida.com', password='P@$$word').GetCookies()
site = Site('https://cocacolaflorida.sharepoint.com/Distribution/', authcookie=authcookie)
sp_list = site.List('Hazardous Material Disclosure Statement') 
data = sp_list.GetListItems()

data_df = pd.DataFrame(data)

data_df = data_df[['Date','Drivers Name', 'Shipment number', 'DSD Location']]
