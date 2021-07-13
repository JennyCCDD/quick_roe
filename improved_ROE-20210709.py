# -*- coding: utf-8 -*-
"""
@author: Mengxuan Chen
@E-mail: chenmx19@mails.tsinghua.edu.cn
@description:
    # 因子计算 improved_ROE
    # 海通证券，选股因子系列研究（ 七十三） ——使用基本面逻辑改进ROE因子
@@revise log:
    2021.07.08. 创建程序

"""

#%%
import pandas as pd
import numpy as np
import time
import datetime
import calendar
from tqdm import tqdm
import os,re
from WindPy import *
w.start()
import warnings
warnings.filterwarnings('ignore')
starttime = datetime.now()

#%%
def new_date(old_date):
    '''
    # 年报数据 12-31 --》 04-30 05-31 06-30 07-31
    # 中报数据 06-30 --》08-31 09-30
    # 三季报数据 09-30 --》10-31 11-30 12-31 01-31 02-27 03-31
    :param old_date: str
    :return: new_date datetime.datetime

    '''
    old_date_ = datetime.strptime(old_date, '%Y-%m-%d')
    month = old_date_.month
    if month == 12:
        return datetime.strptime(old_date[:5]+'04-30','%Y-%m-%d')
    elif month == 3:
        return datetime.strptime(old_date[:5]+'04-30','%Y-%m-%d')
    elif month == 6:
        return datetime.strptime(old_date[:5]+'08-31','%Y-%m-%d')
    elif month == 9:
        return datetime.strptime(old_date[:5]+'10-31','%Y-%m-%d')

# new_date('2010-03-31')

#%%
def gen_season_index(code,index,start_date,end_date):
    '''
    读取季度的财务数据，按照财报最晚公布日期更新，每月填充，保证没有未来数据
    :param code: str
    :param index: str
    :param start_date: str
    :param end_date: str
    :return:
    '''
    data_set = w.wsd("%s"%code, "%s"%index, start_date, end_date, "Period=Q;Days=Alldays")
    data = data_set.Data
    times = data_set.Times
    df = pd.DataFrame([data[0]]).T
    df.columns = ["%s"%index]
    df.index = times
    df['date'] = df.index.copy()
    df['date'] = df['date'].apply(lambda x: new_date(str(x)))
    df = df.drop_duplicates(subset=['date'],keep='first')
    df.index = df['date']
    df.drop(['date'],axis=1,inplace=True)
    df.dropna(inplace=True)
    if len(df)>0:
        df = pd.concat([df,pd.DataFrame(index=pd.date_range(start=start_date, end=end_date, freq='M'))],axis=1)
    else:
        df = pd.concat([pd.DataFrame([np.nan]*len(df)), pd.DataFrame(index=pd.date_range(start=start_date, end=end_date, freq='M'))], axis=1)
    df.fillna(method='ffill',inplace=True)
    df["%s"%index] = df["%s"%index].apply(lambda x: x*0.01)
    return df

# DF = gen_season_index('000002.SZ','roe_basic','2020-01-01','2021-06-30')

#%%
def gen_performance_express_roe(code,start_date,end_date):
    '''
    # 业绩快报首次披露日以及roe，取的是每月最后一天为日期
    :param code:
    :param start_date:
    :param end_date:
    :return:
    '''
    data = w.wsd("%s"%code, 'performanceexpress_perfexroediluted,performanceexpress_date', start_date, end_date, "unit=1;dataType=0;Period=Q;Days=Alldays")
    df = pd.DataFrame([data.Data[0],data.Data[1]]).T
    df.index = data.Times
    df.columns = ['业绩快报ROE','业绩快报首次披露日']
    df['date'] = df['业绩快报首次披露日'].apply(lambda x: datetime(x.year, x.month, calendar.monthrange(x.year, x.month)[1]))
    df.drop_duplicates(subset=['date'], keep='last', inplace=True)
    df.index = df['date']
    df.drop(['业绩快报首次披露日','date'],axis=1,inplace=True)
    df['业绩快报ROE'] = df['业绩快报ROE'].apply(lambda x: x * 0.01)

    return df

# DF = gen_performance_express_roe('600958.SH','2010-01-01','2021-03-31')
#%%
def gen_index_constitutent(sector_id,date):
    '''
    沪深300 a001030201000000
    全部A股 a001010100000000
    :param sector_id:
    :param date:
    :return:
    '''
    data = w.wset("sectorconstituent","date=%s"%date+";sectorid=%s"%sector_id)
    df = pd.DataFrame([data.Data[1], data.Data[2]]).T
    df.index = data.Data[0]
    df.columns = ['code','name']
    return df
stocks = gen_index_constitutent('a001010100000000','2021-07-08')

#%%
DF = pd.DataFrame()
for stock_i in tqdm(stocks['code'].tolist()):
    df1 = gen_season_index(stock_i, 'roe_basic', '2011-01-01', '2021-06-30')
    df2 = gen_performance_express_roe(stock_i, '2011-01-01', '2021-06-30')
    df = pd.concat([df1, df2], axis=1)
    df['roe'] = df['roe_basic'].copy()
    df['over_confidence'] = ''
    for i in range(1, len(df.index)):
        series = df.iloc[i, :]
        if series['业绩快报ROE'] > 0:
            df['roe'].iloc[i] = series['业绩快报ROE']


        # series_last = df.iloc[i - 1, :]
        # if series_last['业绩快报ROE'] > series['roe_basic']:
        #     df['over_confidence'].iloc[i] = series_last['业绩快报ROE'] - series['roe_basic']

    DF = DF.append(df['roe'])
DF.index = stocks['code'].tolist()
DF.dropna(how='all',inplace=True)

#%%
DF1 = pd.read_csv('DF.csv',index_col=0)
DF2 = pd.read_csv('DF2.csv',index_col=0)
DF1.index = stocks['code'].tolist()[:2608]
DF2.INDEX = stocks['code'].tolist()[2608:2608+1445]
DF = DF1.append(DF2)
#%%
DFF = pd.DataFrame()
for i in DF.index:
    df_I = pd.DataFrame({'TRADE_DT':DF.columns,
                         'improved_ROE':DF.loc[i,:],
                         'S_INFO_WINDCODE':[i]*len(DF.columns)})
    DFF = DFF.append(df_I)

DFF.to_csv('improved_ROE.csv')

#%%
# 计算程序运行时间
endtime = datetime.datetime.now()


def timeStr(s):
    if s < 10:
        return '0' + str(s)
    else:
        return str(s)


print("程序开始运行时间：" + timeStr(starttime.hour) + ":" + timeStr(starttime.minute) + ":" + timeStr(starttime.second))
print("程序结束运行时间：" + timeStr(endtime.hour) + ":" + timeStr(endtime.minute) + ":" + timeStr(endtime.second))
runTime = (endtime - starttime).seconds
runTimehour = runTime // 3600  # 除法并向下取整，整除
runTimeminute = (runTime - runTimehour * 3600) // 60
runTimesecond = runTime - runTimehour * 3600 - runTimeminute * 60
print("程序运行耗时：" + str(runTimehour) + "时" + str(runTimeminute) + "分" + str(runTimesecond) + "秒")