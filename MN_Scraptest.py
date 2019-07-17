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
from multiprocessing import Process as pr

indexurl = 'http://www.moneycontrol.com/india/stockpricequote'
prefix='http://www.moneycontrol.com/india'


# columns=['datekey','url','title','bseid','nseid','isinid','bsetrad_day','bsecurrentprice','bseprevdayprice','bseopenprice','todaybselow','todaybsehigh','bsevol','bse52weeklow','bse52weekhigh',
#          'nsetrad_day','nsecurrentprice','nseprevdayprice','nseopenprice','todaynselow','todaynsehigh','nsevol','nse52weeklow','nse52weekhigh',
#          'tsharecaptital_val','tnetworth_val','tneteblock_val','tinvestments_val','tassets_val']
now = datetime.datetime.now()
year=now.year
date=now.date()
csvname= '/home/jithendra/Downloads/'+'mn_'+str(date)+'.csv'

def sqlconn():

    connection = create_engine("mysql+pymysql://root:toor@localhost/test1", encoding='utf8', convert_unicode=True)
    return  connection

def getindexurls(args1):

    r = requests.get(args1)
    soup = BeautifulSoup(r.content, 'lxml')

    indexurl_list = []
    data = soup.findAll('div', attrs={'class': 'MT2 PA10 brdb4px alph_pagn'})
    for div in data:
        links = div.findAll('a')
        for a in links:
            indexurl = prefix + a['href']
            # print prefix + a['href']
            indexurl_list.append(indexurl)
        # print indexurl_list

    return indexurl_list

def getallurs(args):
# for indexurlss in indexurl_list:
    r = requests.get(args)
    soup = BeautifulSoup(r.content, 'lxml')
    df_urls = pd.DataFrame()
    url_list = []
    allurls_list = []
    for urls in soup.find_all('a', href=re.compile('http://www.moneycontrol.com/india/stockpricequote/.*/(.*)$')):
        # print urls
        url = urls['href']
        # print url
        url_list.append(url)
    return url_list

def pagecontent(args):

    page = requests.get(args)
    tree = html.fromstring(page.content)

    return tree

def generaldetails(args):
    s_data = []
    tree=pagecontent(args)
    df = pd.DataFrame()
    bseid=''
    nseid=''
    ISINid=''
    # page = requests.get(args)
    # tree = html.fromstring(page.content)
    title = tree.xpath('// *[ @ id = "nChrtPrc"] / div[3] / h1/text()')
    title = ''.join(title)
    sector = tree.xpath('//a[@class="gry10"] /text()')
    sector = ''.join(sector)
    compinfo = tree.xpath('//div[@class="FL gry10"]/text()')

    for bse in compinfo :
        if 'BSE:' in bse:
            bseid= bse.split("BSE:", 1)[1]
        if 'NSE:' in bse:
            nseid=bse.split("NSE:", 1)[1]
        if 'ISIN:' in bse:
            ISINid=bse.split("ISIN:", 1)[1]

    s_data.append({'title':title,'sector':sector,'bseid': bseid,'nseid':nseid,'ISINid':ISINid})
    df = pd.DataFrame(s_data)

    # todays_prices1 = [title,''.join(bseid),''.join(nseid),''.join(ISINid),','.join(sector)]

    # print df.head()
    return df
    # print title
    # print bseid
    # print nseid

def getbseinfo(args):
            s_data = []
            df = pd.DataFrame()
            tree = pagecontent(args)

            # page = requests.get(args)
            # tree = html.fromstring(page.content)
            bsetrad_day = tree.xpath('//div[@id="bse_upd_time" and @class="CL"]/text()')
            bsetrad_day=''.join(bsetrad_day)
            bsetrad_day=bsetrad_day.replace(",","")
            bsetrad_day = str(year) + ' ' + bsetrad_day
            bsecurrentprice = tree.xpath('//span[@id="Bse_Prc_tick"]/strong/text()')
            bsecurrentprice=''.join(bsecurrentprice)
            bseprevdayprice = tree.xpath('//div[@id="b_prevclose" and @class="gD_12 PB3"]/strong/text()')
            bseprevdayprice=''.join(bseprevdayprice)
            bseopenprice = tree.xpath('//div[@id="b_open" and @class="gD_12 PB3"]/strong/text()')
            bseopenprice=''.join(bseopenprice)
            todaybselow = tree.xpath('//span[@id="b_low_sh" and @class="PR5"]/text()')
            todaybselow=''.join(todaybselow)
            todaybsehigh1 = tree.xpath('//span[@id="b_high_sh" and @class="PL5"]/text()')
            todaybsehigh1=''.join(todaybsehigh1)
            todaybsehigh2 = tree.xpath('//*[@id="b_high_sh"]/span/text()')
            todaybsehigh2 = ''.join(todaybsehigh2)
            if len(todaybsehigh1) > 1:
                todaybsehigh = todaybsehigh1
            else:
                todaybsehigh = todaybsehigh2

            bsevol = tree.xpath('//span[@id="bse_volume" and @class="gD_12"]/strong/text()')
            bsevol=''.join(bsevol)
            bsevol = bsevol.replace(",", "")
            bse52weeklow = tree.xpath('//span[@id="b_52low" and @class="PR5"]/text()')
            bse52weeklow=''.join(bse52weeklow)
            bse52weekhigh1 = tree.xpath('//span[@id="b_52high" and @class="PL5"]/text()')
            bse52weekhigh2 = tree.xpath('// *[ @ id = "b_52high"] / span/text()')
            bse52weekhigh1=''.join(bse52weekhigh1)
            bse52weekhigh2 = ''.join(bse52weekhigh2)
            if len(bse52weekhigh1)>1:
                bse52weekhigh=bse52weekhigh1
            else:
                bse52weekhigh = bse52weekhigh2


            s_data.append({'bsetrad_day': bsetrad_day, 'bsecurrentprice': bsecurrentprice, 'bseprevdayprice': bseprevdayprice, 'bseopenprice': bseopenprice, 'todaybselow': todaybselow,'todaybsehigh':todaybsehigh,'bsevol':bsevol,'bse52weeklow':bse52weeklow,'bse52weekhigh':bse52weekhigh})
            df1 = pd.DataFrame(s_data)
            print df1.head()
            return df1
            # d = datetime.strptime('2007-07-18 10:03:19', '%Y-%m-%d %H:%M:%S')
            # day_string = d.strftime('%Y-%m-%d')
            # todays_prices = [format_tday(bsetrad_day),','.join(bsecurrentprice), ','.join(bseprevdayprice), ','.join(bseopenprice), ','.join(todaybselow),','.join(todaybsehigh),
            #                  ','.join(bsevol),','.join(bse52weeklow),','.join(bse52weekhigh)]

            # print url
            # print bsecurrentprice
            # print bseopenprice
            # return bse52weeklow
            # return bse52weekhigh



    # print "next"

def getBalancesheet(args):
    s_data = []
    df = pd.DataFrame()
    tree=pagecontent(args)

    # page = requests.get(arg)
    # tree = html.fromstring(page.content)
    # tsharecaptital = tree.xpath('//div[@class="FR w252"]/div[1]/table/tr[1]/td[1]/text()')
    tsharecaptital_val = tree.xpath('//div[@class="FR w252"]/div[1]/table/tr[1]/td[2]/span/text()')
    tsharecaptital_val = ''.join(tsharecaptital_val)
    tsharecaptital_val = tsharecaptital_val.replace(",", "")
    # tnetworth = tree.xpath('//div[@class="FR w252"]/div[1]/table/tr[2]/td[1]/text()')
    tnetworth_val = tree.xpath('//div[@class="FR w252"]/div[1]/table/tr[2]/td[2]/span/text()')
    tnetworth_val = ''.join(tnetworth_val)
    tnetworth_val=tnetworth_val.replace(",","")
    # tdeb = tree.xpath('//div[@class="FR w252"]/div[1]/table/tr[3]/td[1]/text()')
    tdeb_val = tree.xpath('//div[@class="FR w252"]/div[1]/table/tr[3]/td[2]/span/text()')
    tdeb_val = ''.join(tdeb_val)
    tdeb_val = tdeb_val.replace(",", "")
    # tneteblock = tree.xpath('//div[@class="FR w252"]/div[1]/table/tr[4]/td[1]/text()')
    tneteblock_val = tree.xpath('//div[@class="FR w252"]/div[1]/table/tr[4]/td[2]/span/text()')
    tneteblock_val = ''.join(tneteblock_val)
    tneteblock_val = tneteblock_val.replace(",", "")
    # tinvestments = tree.xpath('//div[@class="FR w252"]/div[1]/table/tr[5]/td[1]/text()')
    tinvestments_val = tree.xpath('//div[@class="FR w252"]/div[1]/table/tr[5]/td[2]/span/text()')
    tinvestments_val = ''.join(tinvestments_val)
    tinvestments_val = tinvestments_val.replace(",", "")
    # tassets = tree.xpath('//div[@class="FR w252"]/div[1]/table/tr[6]/td[1]/text()')
    tassets_val = tree.xpath('//div[@class="FR w252"]/div[1]/table/tr[6]/td[2]/span/text()')
    tassets_val = ''.join(tassets_val)
    tassets_val = tassets_val.replace(",","")
    pe_val=tree.xpath('//*[@id="mktdet_1"]/div[1]/div[2]/div[2]/text()')
    pe_val = ''.join(pe_val)
    book_val = tree.xpath('//*[@id="mktdet_1"]/div[1]/div[3]/div[2]/text()')
    book_val = ''.join(book_val)
    face_val = tree.xpath('//*[@id="mktdet_1"]/div[2]/div[5]/div[2]/text()')
    face_val = ''.join(face_val)
    industry_pe_val = tree.xpath('//*[@id="mktdet_1"]/div[1]/div[6]/div[2]/text()')
    industry_pe_val= ''.join(industry_pe_val)
    latest_share_pattern = tree.xpath('//table//td[@class="thc04 w90 gD_12 tar"][1]/span/text() ')
    Promoter_share = latest_share_pattern[0]
    public_share = latest_share_pattern[1]
    other_share = latest_share_pattern[2]
    s_data.append({'tsharecaptital_val': tsharecaptital_val, 'tnetworth_val': tnetworth_val, 'tdeb_val': tdeb_val,'tneteblock_val': tneteblock_val, 'tinvestments_val': tinvestments_val,'tassets_val': tassets_val,
                   'pe_val':pe_val,'book_val':book_val,'face_val':face_val,'industry_pe_val':industry_pe_val,'Promoter_share':Promoter_share,'public_share':public_share,'other_share':other_share})

    df4 = pd.DataFrame(s_data)
    return df4
    # print df.head()
    # todays_prices = [tsharecaptital_val, ','.join(tnetworth_val), ','.join(tdeb_val),','.join(tneteblock_val), ','.join(tinvestments_val), ','.join(tassets_val)]

    # print todays_prices
    # return tinvestments_val
    # return tassets_val

def nsefino(args):
    s_data = []
    df = pd.DataFrame()
    tree = pagecontent(args)
    # page = requests.get(arg)
    # tree = html.fromstring(page.content)
    nsetrad_day = tree.xpath('//div[@id="nse_upd_time" and @class="CL"]/text()')
    nsetrad_day=''.join(nsetrad_day)
    nsetrad_day=nsetrad_day.replace(",","")
    nsetrad_day = str(year)+' '+nsetrad_day
    nsecurrentprice = tree.xpath('//span[@id="Nse_Prc_tick" and @class="PA2"]/strong/text()')
    nsecurrentprice = ''.join(nsecurrentprice)
    nseprevdayprice = tree.xpath('//div[@id="n_prevclose" and @class="gD_12 PB3"]/strong/text()')
    nseprevdayprice = ''.join(nseprevdayprice)
    nseopenprice = tree.xpath('//div[@id="n_open" and @class="gD_12 PB3"]/strong/text()')
    nseopenprice = ''.join(nseopenprice)
    todaynselow = tree.xpath('//span[@id="n_low_sh" and @class="PR5"]/text()')
    todaynselow = ''.join(todaynselow)
    todaynsehigh1 = tree.xpath('//span[@id="n_high_sh" and @class="PL5"]/text()')
    todaynsehigh1 = ''.join(todaynsehigh1)
    todaynsehigh2 = tree.xpath('//*[@id="n_high_sh"]/span/text()')
    todaynsehigh2 = ''.join(todaynsehigh2)

    if len(todaynsehigh1) > 1:
        todaynsehigh = todaynsehigh1
    else:
        todaynsehigh = todaynsehigh2

    nsevol = tree.xpath('//span[@id="nse_volume" and @class="gD_12"]/strong/text()')
    nsevol = ''.join(nsevol)
    nsevol = nsevol.replace(",", "")
    nse52weeklow = tree.xpath('//span[@id="n_52low" and @class="PR5"]/text()')
    nse52weeklow = ''.join(nse52weeklow)
    nse52weekhigh1 = tree.xpath('//span[@id="n_52high" and @class="PL5"]/text()')
    nse52weekhigh1 = ''.join(nse52weekhigh1)
    nse52weekhigh2 = tree.xpath('//*[@id="n_52high"]/span/text()')
    nse52weekhigh2 = ''.join(nse52weekhigh2)
    if len(nse52weekhigh1) > 1:
        nse52weekhigh = nse52weekhigh1
    else:
        nse52weekhigh = nse52weekhigh2
    s_data.append({'nsetrad_day': nsetrad_day, 'nsecurrentprice': nsecurrentprice, 'nseprevdayprice': nseprevdayprice,'nseopenprice': nseopenprice, 'todaynselow': todaynselow,'todaynsehigh': todaynsehigh, 'nsevol': nsevol, 'nse52weeklow': nse52weeklow,'nse52weekhigh': nse52weekhigh})
    df3 = pd.DataFrame(s_data)
    # print df3.head()
    return df3
    # todays_prices = [format_tday(nsetrad_day), ','.join(nsecurrentprice), ','.join(nseprevdayprice),
    #                  ','.join(nseopenprice), ','.join(todaynselow), ','.join(todaynsehigh),
    #                  ','.join(nsevol), ','.join(nse52weeklow), ','.join(nse52weekhigh)]
    #
    # print todays_prices
    # return nse52weekhigh
def gatherdata(allurls):
    for url in allurls:
        for url1 in url:
            print url1
            # print url

            # s_data.append({'url': url})
            # df = pd.DataFrame(s_data)
            # print df.head()

            try:

                tree = pagecontent(url1)
                df = generaldetails(url1)
                df1 = getbseinfo(url1)
                df3 = nsefino(url1)
                df4 = getBalancesheet(url1)
                # print df.head()
                # generaldetails(url)
                # getbseinfo(url)
                # nsefino(url)
                # getBalancesheet(url)
                #     result=pd.DataFrame()
                # result=pd.concat([df,df1,df3,df4], axis=1)
                finalpd = pd.concat([df, df1, df3, df4], axis=1)
                print "start print"
                print finalpd.head()
                print "print done"
                # result.to_csv()
                finalpd['Datekey'] = date
                finalpd['URL'] = url1
                # finalpd=finalpd.append(finalpd)
                if not os.path.isfile(csvname):
                    finalpd.to_csv(csvname, header='column_names', index=False)
                else:  # else it exists so append without writing the header
                    finalpd.to_csv(csvname, mode='a', header=False, index=False)

            except:
                pass
                # if

def m_gdata(url1):
    try:
        r = requests.get(url1)
        actual_url=(r.url)
        print actual_url
        df = generaldetails(actual_url)
        df1 = getbseinfo(actual_url)
        df3 = nsefino(actual_url)
        df4 = getBalancesheet(actual_url)

        finalpd = pd.concat([df, df1, df3, df4], axis=1)
        print "start print"
        print finalpd.head()
        print "print done"
        # result.to_csv()
        finalpd['Datekey'] = date
        finalpd['URL'] = actual_url
        if not os.path.isfile(csvname):
            finalpd.to_csv(csvname, header='column_names', index=False)
        else:  # else it exists so append without writing the header
            finalpd.to_csv(csvname, mode='a', header=False, index=False)
    except Exception as e:
        print e
        pass
if __name__ == "__main__":

    indexurl_list = getindexurls(indexurl)
    allurls = []
    print "index Urls",indexurl_list
    #
    # for indexurlss in islice(indexurl_list, 1, None):
    for indexurlss in indexurl_list:
        i=0
        if len(indexurl_list)>0:
            print indexurlss
            url_list1 = getallurs(indexurlss)

        # allurls.append(url_list)
            allurls.extend(url_list1)
    print "No Of All urls : ",len(allurls)

    # for url in allurs:
    #     for url1 in url:
    #         print url1
    # url_list = getallstockruls()Sreee@2Jitu
    connection = sqlconn()
    # print url_list
    try:
        os.remove(csvname)
    except OSError:
        pass
    # CommandOutput = os.remove('d:/test.csv')

    # allurls1=["http://www.moneycontrol.com/india/stockpricequote/powertransmissionequipment/a2zinfraengineering/AME02",'http://www.moneycontrol.com/india/stockpricequote/financegeneral/akcapitalservices/AKC01','http://www.moneycontrol.com/india/stockpricequote/textilesspinningcottonblended/aptyarns/APT01','http://www.moneycontrol.com/india/stockpricequote/miscellaneous/akspintex/AKS01','http://www.moneycontrol.com/india/stockpricequote/financeleasinghirepurchase/akscredits/AKS','http://www.moneycontrol.com/india/stockpricequote/cementproductsbuildingmaterials/ainfrastructure/AI74','http://www.moneycontrol.com/india/stockpricequote/miscellaneous/afenterprises/AFE01','http://www.moneycontrol.com/india/stockpricequote/miscellaneous/amjumbobags/JB03']
    # for url in url_list:
    starttime = time.time()
    try:

        print "StartTime:"+str(starttime)
        p = Pool(20)
        p.map(m_gdata, allurls)
        p.terminate()
        p.join()
        # pool = Pool(processes=5)

        # pool.apply_async(gatherdata(allurls1))
        # gatherdata(allurls1)
        # p=pr(target=gatherdata, args=('allurls1'))
        # p.close()
        endtime =  time.time()
        print "StartTime:" + str(starttime)
        print "EndTime:" + str(endtime)
        print "Total execution time (Mins) :",(endtime-starttime)/60
    except Exception as e:
        print "Total execution time (Mins):",((time.time()-starttime)/60)