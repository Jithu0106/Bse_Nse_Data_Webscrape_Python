# MN_Scrape
## Jithendra
## a.jithendra@gmail.com

BSE NSE Data Python Webscraping from Moneycontrol 
This code works with python 2.7 only. If you have python 3+ then change all the print statement syntax
Install the below python libraries as pre-requisite

import requests
from bs4 import BeautifulSoup
import pandas as pd
from lxml import html
from sqlalchemy import create_engine
import datetime
import re
import os
from itertools import islice
import time
from multiprocessing import Pool
