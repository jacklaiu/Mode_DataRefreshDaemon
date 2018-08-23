import base.Util as util
import base.Dao as dao
import base.Log as log
import tushare as ts
import time

def daemon_refresh_security_daily():
    log.log("Start daemon_refresh_security_daily")
    while True:
        yesterday = util.getPreDayYMD(1, util.getYMD())
        if util.isOpen(yesterday) is False or util.getHMS() < "04:50:00" or util.getHMS() > "04:52:00":
            time.sleep(10)
            continue
        log.log("refresh securities daily base data")
        max_date_db = dao.select("select max(date) max_date from t_security_daily", ())[0]['max_date']
        if max_date_db is None:
            start = util.preOpenDate(util.getLastestOpenDate(), 150)
            end = util.getLastestOpenDate()
        else:
            start = max_date_db
            end = util.getLastestOpenDate()

        if start == end:
            log.log("start == end, so go to sleep")
            time.sleep(600)
            continue

        securitys = [item['code'] for item in dao.select("select distinct code from t_security_concept", ())]
        values = []
        for code in securitys:
            df = ts.get_k_data(code, start, end)
            pre_close = None
            for items in df.iterrows():
                row = items[1]
                if pre_close is None:
                    pre_close = row['close']
                    continue
                date = row['date']
                if date == max_date_db:
                    continue
                code = row['code']
                open = row['open']
                close = row['close']
                high = row['high']
                low = row['low']
                log.log("Values Len:" + str(values.__len__()) + " Code: " + code + " Date: " + str(date))
                values.append((code, pre_close, high, close, low, open, date))
                pre_close = close
                if values.__len__() == 250000:
                    log.log("saving 2 db ing...")
                    dao.updatemany(
                        "insert into t_security_daily(code, pre_close, high, close, low, open, date) values(%s,%s,%s,%s,%s,%s,%s)",
                        values)
                    values = []
        log.log("saving 2 db ing...")
        dao.updatemany(
            "insert into t_security_daily(code, pre_close, high, close, low, open, date) values(%s,%s,%s,%s,%s,%s,%s)",
            values)
        log.log("saving 2 db finished!")
        time.sleep(60*100)

daemon_refresh_security_daily()

