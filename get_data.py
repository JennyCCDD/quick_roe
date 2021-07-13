# -*- coding: utf-8 -*-
import time
import datetime
import os
import pandas as pd
import numpy as np
from db_operation import DBOperations
from db_credential import credentials, oracle_credentials
# 连接数据库
db_opt_wind = DBOperations(**oracle_credentials)

# =============================================================================
# 取交易日期
# =============================================================================
sql1 = '''
           select distinct acal.TRADE_DAYS
           from wind.AShareCalendar acal
           where acal.S_INFO_EXCHMARKET = 'SSE'
    '''

enddate = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%Y%m%d")
trad_date = db_opt_wind.read_sql(sql1).sort_values(by='TRADE_DAYS', ascending=True).set_index(
    keys="TRADE_DAYS", drop=False).loc["20031231":enddate, :]
if not os.path.exists('./data'):
    os.mkdir('./data')
trad_date.to_csv('./data/trad_date.csv')


# %%===========================================================================
# Wind一致预期
# -----------------------------------------------------------------------------
# Wind一致预测个股滚动指标[AShareConsensusRollingData]
sql2 = '''select S_INFO_WINDCODE, EST_DT, ROLLING_TYPE, NET_PROFIT, EST_EPS, EST_PE, EST_PEG, EST_PB, EST_ROE
         EST_OPER_REVENUE, EST_CFPS, EST_DPS, EST_BPS, EST_EBIT, EST_EBITDA, EST_TOTAL_PROFIT, EST_OPER_PROFIT, 
         EST_OPER_COST, BENCHMARK_YR, EST_BASESHARE
         from AShareConsensusRollingData order by EST_DT
      '''
ConsensusRollingData = db_opt_wind.read_sql(sql2)
ConsensusRollingData.to_csv('./data/ConsensusRollingData.csv')

# %%===========================================================================
# 交易信息
# -----------------------------------------------------------------------------
# 中国A股停复牌信息[AShareTradingSuspension]
sql3 = '''select S_INFO_WINDCODE, S_DQ_SUSPENDDATE, S_DQ_RESUMPDATE
         from AShareTradingSuspension order by S_DQ_SUSPENDDATE
      '''
tradeornot = db_opt_wind.read_sql(sql3)
tradeornot.to_csv('./data/tradeornot.csv')

# %%===========================================================================
# 交易信息
# -----------------------------------------------------------------------------
# 中国A股日行情估值指标[AShareEODDerivativeIndicator]
sql4 = '''select S_INFO_WINDCODE, TRADE_DT, S_VAL_PE_TTM, S_VAL_PB_NEW, S_VAL_PCF_OCFTTM, S_VAL_PS_TTM, S_DQ_FREETURNOVER,
          S_DQ_CLOSE_TODAY, UP_DOWN_LIMIT_STATUS
         from AShareEODDerivativeIndicator order by TRADE_DT
      '''
updownlimitstatus = db_opt_wind.read_sql(sql4)
updownlimitstatus.to_csv('./data/updownlimitstatus.csv')


# %%===========================================================================
# 资产负债表
# -----------------------------------------------------------------------------
# 中国A股资产负债表[AShareBalanceSheet]
sql5 = '''select S_INFO_WINDCODE, REPORT_PERIOD, TOT_SHRHLDR_EQY_EXCL_MIN_INT, ACTUAL_ANN_DT,MONETARY_CAP, ST_BORROW, 
          BONDS_PAYABLE, LT_PAYABLE
         from AShareBalanceSheet order by REPORT_PERIOD
      '''
balancesheet = db_opt_wind.read_sql(sql5)
balancesheet.to_csv('./data/balancesheet.csv')

# %%===========================================================================
# 利润表
# -----------------------------------------------------------------------------
# 中国A股利润表[AShareIncome]
sql6 = '''select S_INFO_WINDCODE, REPORT_PERIOD, EBIT, TOT_PROFIT, INC_TAX
         from AShareIncome order by REPORT_PERIOD
      '''
profitloss = db_opt_wind.read_sql(sql6)
profitloss.to_csv('./data/profitloss.csv')
