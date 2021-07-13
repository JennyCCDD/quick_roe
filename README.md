# quick_roe

## 处理财报数据滞后

```python
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
```

## 获取财报数据

```python
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
```

## 获取指数成分股

```python
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
```

