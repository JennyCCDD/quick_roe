# -*- coding: utf-8 -*-
"""
@author: Mengxuan Chen
@E-mail: chenmx19@mails.tsinghua.edu.cn
@description:
    # 因子计算 improved_ROE
@@revise log:
    2021.07.09. 创建程序 wind excel提取数据而非api接口调用

"""


#%%
import pandas as pd
import numpy as np
import time
from datetime import datetime
import calendar
from tqdm import tqdm
import os,re
# import warnings
# warnings.filterwarnings('ignore')
starttime = datetime.now()
os.chdir('E:/令人心动的offer/我的进度条/！华夏基金summer-20210421/factors/improved_ROE')

def new_date(old_date):
    '''
    # 年报数据 12-31 --》 04-30 05-31 06-30 07-31
    # 中报数据 06-30 --》08-31 09-30
    # 三季报数据 09-30 --》10-31 11-30 12-31 01-31 02-27 03-31
    :param old_date: str
    :return: new_date datetime.datetime

    '''
    old_date_ = datetime.strptime(old_date, '%Y/%m/%d')
    month = old_date_.month
    if month == 12:
        return datetime.strptime(str(int(old_date[:4])+1) + '/4/30','%Y/%m/%d')
    elif month == 3:
        return datetime.strptime(old_date[:5]+'4/30','%Y/%m/%d')
    elif month == 6:
        return datetime.strptime(old_date[:5]+'8/31','%Y/%m/%d')
    elif month == 9:
        return datetime.strptime(old_date[:5]+'10/31','%Y/%m/%d')



#%%
# 股票池筛选
isExists_roe = os.path.exists('./result/ROE_orginal.csv')
if not isExists_roe:
    ROE = pd.read_csv('./data/ROE.csv', index_col=0)
    ROE['date'] = ROE.index.copy()
    ROE['date'] = ROE['date'].apply(lambda x: new_date(str(x)))
    ROE = ROE.drop_duplicates(subset=['date'], keep='first')
    ROE.index = ROE['date']
    ROE.drop(['date'], axis=1, inplace=True)
    ROE.dropna(how='all', inplace=True, axis=0)
    ROE = pd.concat([ROE, pd.DataFrame(index=pd.date_range(start='2004-04-30', end='2021-06-30', freq='M'))], axis=1)
    ROE.fillna(method='ffill', axis=0, inplace=True)
    ROE = ROE.apply(lambda x: x * 0.01)
    if not os.path.exists('./result'):
        os.mkdir('./result')
    ROE.to_csv('./result/ROE_orginal.csv')
else:
    ROE = pd.read_csv('./result/ROE_orginal.csv',index_col=0)

isExists_roe_plus_express = os.path.exists('./result/ROE_plus_express.csv')
if not isExists_roe_plus_express:
    ROE_express = pd.read_csv('./data/业绩快报-ROE.csv', index_col=0)
    ROE_express_date = pd.read_csv('./data/业绩快报最新公告日-ROE.csv', index_col=0)
    ROE_plus_express = pd.DataFrame()
    for stock_i in ROE_express.columns.tolist():
        df = pd.concat([ROE_express[stock_i], ROE_express_date[stock_i]], axis=1)
        df.columns = ['业绩快报ROE', '业绩快报最新披露日']
        df['业绩快报ROE'] = df['业绩快报ROE'].apply(lambda x: x * 0.01)
        df.dropna(axis=0, inplace=True)
        df['业绩快报最新披露日'] = df['业绩快报最新披露日'].apply(lambda x: datetime.strptime(str(x), '%Y/%m/%d'))
        df['date'] = df['业绩快报最新披露日'].apply(lambda x: datetime(x.year, x.month, calendar.monthrange(x.year, x.month)[1]))
        df.index = df.date.copy()
        df.drop(['业绩快报最新披露日', 'date'], axis=1, inplace=True)
        df = pd.concat([df, ROE[stock_i]], axis=1)
        df.columns = ['业绩快报ROE', 'ROE_basic']
        df[stock_i] = df['ROE_basic'].copy()
        for i in range(1, len(df.index)):
            if df.iloc[i, :]['业绩快报ROE'] > 0:
                df[stock_i].iloc[i] = df.iloc[i, :]['业绩快报ROE']
        ROE_plus_express = ROE_plus_express.append(df[stock_i])
    ROE_plus_express.to_csv('./result/ROE_plus_express.csv')
else:
    ROE_plus_express = pd.read_csv('./result/ROE_plus_express.csv',index_col=0)

isExists_roe_plus_report = os.path.exists('./result/ROE_plus_report.csv')
if not isExists_roe_plus_report:
    np_report_high = pd.read_csv('./data/业绩预告净利润上限.csv', index_col=0)
    np_report_low = pd.read_csv('./data/业绩预告净利润下限.csv', index_col=0)
    report_date = pd.read_csv('./data/业绩预告日.csv', index_col=0)
    equity = pd.read_csv('./data/equity.csv', index_col=0)
    equity['date'] = equity.index.copy()
    equity['date'] = equity['date'].apply(lambda x: new_date(str(x)))
    equity = equity.drop_duplicates(subset=['date'], keep='first')
    equity.index = equity['date']
    equity.drop(['date'], axis=1, inplace=True)
    equity.dropna(how='all', inplace=True, axis=0)
    equity = pd.concat([equity, pd.DataFrame(index=pd.date_range(start='2004-04-30', end='2021-06-30', freq='M'))],
                       axis=1)
    equity.fillna(method='ffill', axis=0, inplace=True)
    ROE_plus_report = pd.DataFrame()
    ROE_plus_express_plus_report = pd.DataFrame()
    for stock_i in report_date.columns.tolist():
        df = pd.concat([np_report_high[stock_i], np_report_low[stock_i], equity[stock_i], report_date[stock_i]], axis=1)
        df.columns = ['业绩预告净利润上限', '业绩预告净利润下限', '权益', '业绩预告日']
        df['权益'].replace(0, np.nan, inplace=True)
        df['业绩预告-ROE'] = df.apply(lambda x: (x['业绩预告净利润上限'] + x['业绩预告净利润下限']) / float(x['权益']), axis=1)
        df.dropna(axis=0, inplace=True)
        df['业绩预告日'] = df['业绩预告日'].apply(lambda x: datetime.strptime(str(x), '%Y/%m/%d'))
        df['date'] = df['业绩预告日'].apply(lambda x: datetime(x.year, x.month, calendar.monthrange(x.year, x.month)[1]))
        df.index = df.date.copy()
        df.drop(['业绩预告日', 'date', '业绩预告净利润上限', '业绩预告净利润下限', '权益'], axis=1, inplace=True)
        df = pd.concat([df, ROE[stock_i]], axis=1)
        df.columns = ['业绩预告ROE', 'ROE_basic']
        df[stock_i] = df['ROE_basic'].copy()
        for i in range(1, len(df.index)):
            if df.iloc[i, :]['业绩预告ROE'] > 0:
                df[stock_i].iloc[i] = df.iloc[i, :]['业绩预告ROE']
        ROE_plus_report = ROE_plus_report.append(df[stock_i])

        df2 = pd.concat([df['业绩预告ROE'], ROE_plus_express.loc[stock_i, :]], axis=1)
        df2.columns = ['业绩预告ROE', 'ROE']
        df2[stock_i] = df2['ROE'].copy()
        for i in range(1, len(df2.index)):
            if df2.iloc[i, :]['业绩预告ROE'] > 0:
                df2[stock_i].iloc[i] = df2.iloc[i, :]['业绩预告ROE']
        ROE_plus_express_plus_report = ROE_plus_express_plus_report.append(df2[stock_i])
    ROE_plus_report.T.to_csv('./result/ROE_plus_report.csv')
    ROE_plus_express_plus_report.T.to_csv('./result/ROE_plus_express_plus_report.csv')
else:
    ROE_plus_report = pd.read_csv('./result/ROE_plus_report.csv',index_col=0)
    ROE_plus_express_plus_report = pd.read_csv('./result/ROE_plus_express_plus_report.csv',index_col=0)

#%%
stornot = pd.read_csv('./data/stornot.csv',index_col=0)
stornot['date'] = stornot.index
stornot['date'] = stornot['date'] .apply(lambda x: datetime.strptime(str(x), '%Y/%m/%d'))
stornot.index = stornot['date']
stornot.drop(['date'],axis=1,inplace=True)

# 以下三张表格待更新
ipo3monMatrix = pd.read_csv('./data/ipo3monMatrix.csv',index_col=0)
zdtInfoMatrix = pd.read_csv('./data/zdtInfoMatrix.csv',index_col=0)
transacInfoMatrix = pd.read_csv('./data/transacInfoMatrix.csv',index_col=0)
allstocks = pd.DataFrame(columns=stornot.columns)
ipo3monMatrix = pd.concat([ipo3monMatrix,allstocks])
zdtInfoMatrix = pd.concat([zdtInfoMatrix,allstocks])
transacInfoMatrix = pd.concat([transacInfoMatrix,allstocks])
ipo3monMatrix['date'] = ipo3monMatrix.index
ipo3monMatrix['date'] = ipo3monMatrix['date'] .apply(lambda x: datetime.strptime(str(x), '%Y-%m-%d'))
ipo3monMatrix.index = ipo3monMatrix['date']
ipo3monMatrix.drop(['date'],axis=1,inplace=True)
zdtInfoMatrix['date'] = zdtInfoMatrix.index
zdtInfoMatrix['date'] = zdtInfoMatrix['date'] .apply(lambda x: datetime.strptime(str(x), '%Y%m%d'))
zdtInfoMatrix.index = zdtInfoMatrix['date']
zdtInfoMatrix.drop(['date'],axis=1,inplace=True)
transacInfoMatrix['date'] = transacInfoMatrix.index
transacInfoMatrix['date'] = transacInfoMatrix['date'] .apply(lambda x: datetime.strptime(str(x), '%Y%m%d'))
transacInfoMatrix.index = transacInfoMatrix['date']
transacInfoMatrix.drop(['date'],axis=1,inplace=True)

#%%
def gen_factor(factor,factor_name):
    DFF = pd.DataFrame()
    for i in tqdm(factor.index):

        df_i = pd.DataFrame({'TRADE_DT':factor.columns,
                             '%s' % factor_name :factor.loc[i,:],
                             'S_INFO_WINDCODE':[i]*len(factor.columns)})
        df_i = pd.concat([df_i,stornot[i],ipo3monMatrix[i],zdtInfoMatrix[i],transacInfoMatrix[i]],axis=1)
        df_i.columns = ['TRADE_DT', '%s' % factor_name, 'S_INFO_WINDCODE', 'stornot', 'ipo3mon', 'zdtInfo',
                        'transacInfo']
        df_i.dropna(subset=['%s' % factor_name],inplace=True)
        df_i = df_i[df_i['stornot'] != True]
        df_i = df_i[df_i['ipo3mon'] != True]
        df_i = df_i[df_i['zdtInfo'] != 1]
        df_i = df_i[df_i['zdtInfo'] != -1]
        df_i = df_i[df_i['transacInfo'] != 0]
        DFF = DFF.append(df_i.loc[:,'TRADE_DT':'S_INFO_WINDCODE'])
        del df_i
    DFF.to_csv('./result/%s' % factor_name + '.csv')
    return DFF

ROE_original_ = gen_factor(ROE.T,'ROE_original_')

ROE_plus_express_ = gen_factor(ROE_plus_express,'ROE_plus_express_')
ROE_plus_report_ = gen_factor(ROE_plus_report.T,'ROE_plus_report_')
ROE_plus_express_plus_report_ = gen_factor(ROE_plus_express_plus_report.T,'ROE_plus_express_plus_report_')

