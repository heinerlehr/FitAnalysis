import pandas as pd
import fitanalysis as gfit

def test_overview():
    ovfm=gfit.OverViewFitMetrics("takeout-20231014T133916Z-001.zip")
    df=ovfm.get_variables_per_timeframe(['min','dis','cal'],'Year',fun=pd.DataFrame.sum)
    assert int(df.dis[2015]) == 243415
    df=ovfm.get_variables_per_timeframe(['min','dis','cal'],'Year',fun=pd.DataFrame.mean)
    assert int(df.cal[2022]) == 2440

def test_daily():
    dfm = gfit.DailyFitMetrics("2015-02-03","takeout-20231014T133916Z-001.zip")
    df = dfm.get_variables_per_time(['min','dis'],'Day',fun=pd.DataFrame.mean)
    assert int(df.dis)==1898