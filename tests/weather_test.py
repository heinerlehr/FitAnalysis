import pandas as pd
import fitanalysis as gfit


def test_load():
    wd = gfit.MeteocatFitWeatherData()
    wd.load()
    assert wd._data is not None

def test_compose():
    wd = gfit.MeteocatFitWeatherData()
    wd.compose(dates=['2022-01-01'])
    assert '2022-01-01' in wd._dates