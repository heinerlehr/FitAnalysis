# -*- coding: utf-8 -*-
# pylint disable=keyword-arg-before-var

import select
import requests
import fitanalysis as gfit
import abc
from abc import ABC, abstractmethod
import pandas as pd
from datetime import datetime
import json
import os
from pathlib import Path
from typing import Union, List, Callable

class WeatherData:
    """
    An abstract class for handling weather data from various APIs.

    Attributes:
    - apikey (APIKey): An instance of APIKey that will be used to get credentials.
    - apikey_name (str): The name of the API Key as stored in the credential store.
    - base_url (str): A base URL for the service.
    - update_url (str): The URL to obtain more data from the API.

    Methods:
    - __init__(self): Initializes the class with the given parameters.
    - get_data(self, url: str, return_json: bool = False) -> Union[pd.DataFrame, dict]: Gets data from the API.
    - get_station_metadata(self, date: str, state: str, query: str) -> pd.DataFrame: Gets metadata for a weather station.
    - find_branches_with_key_value(self, data: dict, key: str, value: str, current_path: list = []) -> list: Finds branches in a dictionary with a given key-value pair.
    """

    apikey = None
    apikey_name = ""
    base_url = ""
    update_url = ""

    def __init__(self):
        """
        Initializes the class with the given parameters.
        """
        self.apikey = gfit.APIKey(self.apikey_name)
    
    def get_data(self, url: str, return_json: bool = False) -> Union[pd.DataFrame, dict]:
        """
        Gets data from the API.

        Args:
        - url (str): The URL to get data from.
        - return_json (bool): Whether to return the data as a JSON object or a Pandas DataFrame.

        Returns:
        - Union[pd.DataFrame, dict]: The data obtained from the API.
        """
        if not url:
            url = self.base_url+self.update_url
        else:
            url = self.base_url+url

        response = requests.get(url, headers={"Content-Type": "application/json", "X-Api-Key": self.apikey.get()})
        if response.status_code != 200:
            raise ValueError(f"Request {url} not successful. Code {response.status_code}. Reason: {response.reason}")
        # Obtain the json
        data = json.loads(response.text)
        # Now read the response into a data frame and return it
        if not return_json:
            data = pd.json_normalize(data)
        return data
    
    @abstractmethod
    def get_station_metadata(self, date: str, state: str, query: str) -> pd.DataFrame:
        """
        Gets metadata for a weather station.

        Args:
        - date (str): The date to get metadata for.
        - state (str): The state to get metadata for.
        - query (str): The query to filter the metadata.

        Returns:
        - pd.DataFrame: The metadata for the weather station.
        """
        pass

    @abstractmethod
    def get_daily_weather(self, dates: list=None, startHour:int=None, endHour:int=None, fun: Callable=None)->pd.DataFrame:
        """
        Returns daily weather data for the specified dates, hours, and function.

        Args:
            dates (list, optional): List of dates in the format '%Y-%m-%d'. Defaults to [datetime.today().strftime("%Y-%m-%d")].
            startHour (int, optional): Starting hour for the data. Defaults to None.
            endHour (int, optional): Ending hour for the data. Defaults to None.
            fun (Callable, optional): Function to apply to the data. Defaults to None.

        Returns:
            pd.DataFrame: DataFrame containing the daily weather data.
        """
        pass
    @abstractmethod
    def load(self)->pd.DataFrame:
        """
        Loads the cached weather data from a csv file.

        Args:
        - None

        Returns:
        - pd.DataFrame: A dataframe containing the loaded weather data, or None if the file does not exist.

        Raises:
        - None
        """
        pass

    @abstractmethod
    def save(self):
        """
        Saves the weather data to a csv file.

        Args:
        - None

        Returns:
        - str: The filename of the saved file.

        Raises:
        - None
        """
        pass
    
    def find_branches_with_key_value(self, data: dict, key: str, value: str, current_path: list = []) -> list:
        """
        Finds branches in a dictionary with a given key-value pair.

        Args:
        - data (dict): The dictionary to search.
        - key (str): The key to search for.
        - value (str): The value to search for.
        - current_path (list): The current path in the dictionary.

        Returns:
        - list: A list of branches in the dictionary with the given key-value pair.
        """
        branches = []
        if isinstance(data, dict):
            if key in data and data[key] == value:
                branches.append({key: value})
            
            for k, v in data.items():
                new_path = current_path + [k]
                branches.extend(self.find_branches_with_key_value(v, key, value, new_path))
        
        return branches

    
class Meteocat(WeatherData):
    """
    A class for handling weather data from the Meteocat API.

    Attributes:
    - apikey_name (str): The name of the API Key as stored in the credential store.
    - base_url (str): A base URL for the service.
    - update_url (str): The URL to obtain more data from the API.

    Methods:
    - get_station_metadata(self, date: str = None, state: str = "ope", query: str = None) -> pd.DataFrame: Gets metadata for a weather station.
    - get_variable_codes(self, vars: list = None) -> pd.DataFrame: Gets the variable codes for the weather station.
    - get_representative_station(self, postcode: str): Gets the representative station for a given postcode.
    """

    apikey_name = "meteocat"
    base_url = 'https://api.meteo.cat/xema/v1'
    update_url = '/municipis'
 
    def get_station_metadata(self, date: str = None, state: str = "ope", query: str = None, return_json:bool=False) -> Union[pd.DataFrame, dict]:
        """
        Gets metadata for a weather station.

        Args:
        - date (str): The date to get metadata for.
        - state (str): The state to get metadata for.
        - query (str): The query to filter the metadata.

        Returns:
        - pd.DataFrame: The metadata for the weather station.
        """
        if not date:
            date = datetime.today().strftime("%Y-%m-%d")
        url = f"/estacions/metadades?estat={state}&data={date}Z"
        sm = self.get_data(url, return_json=return_json)
        if return_json:
            return sm

        if not query:
            return sm

        return sm.query(query)

    def get_specific_station_metadata(self, station_code: str = "", return_json:bool=False) -> Union[pd.DataFrame, dict]:
        if not station_code:
            station_code = self.weather_station
        url = f"/estacions/{station_code}/metadades"
        return self.get_data(url, return_json=return_json)

    def get_variable_codes(self, vars: list = None) -> pd.DataFrame:
        """
        Gets the variable codes for the weather station.

        Args:
        - vars (list): The variables to get codes for.

        Returns:
        - pd.DataFrame: The variable codes for the weather station.
        """
        url = "/variables/mesurades/metadades"
        #url = "/representatives/metadades/variables"
        df = self.get_data(url)
        if not vars:
            # get all variable codes
            return df
        
        namelist = list(df['nom'])
        l = [name for name in namelist if name in vars]
        return df[df.nom.isin(l)]

    def get_representative_station(self, postcode: str):
        """
        Gets the representative station for a given postcode.

        Args:
        - postcode (str): The postcode to get the representative station for.
        """
        # Meteocat stores representative stations against Temperature with code = 32
        variable_code = 32
        
        url = f"/representatives/metadades/municipis/{postcode}/variables/{variable_code}"
        df = self.get_data(url)
        return df
 
    def get_variable_statistics(self, variable_code: int = -1, station_code: str = None) -> pd.DataFrame:
        if variable_code == -1:
            variable_code = 3000  # Temperature
        if not station_code:
            station_code = self.weather_station
        if not station_code:
            url = f"/variables/estadistics/anuals/{variable_code}"
        else:
            url = f"/variables/estadistics/anuals/{variable_code}?codiEstacio={station_code}"
        df = self.get_data(url, return_json=True)
        return df

class MeteocatFitWeatherData(Meteocat):
    """
    A class for handling weather data from the Meteocat API.

    Attributes:
    - weather_station (str): The code for the weather station to use. Default is 'YR'.
    - weather_variables (pd.DataFrame): A dataframe containing information about the weather variables available from the API.
    - _base_dir (str): The base directory for storing the weather data. Default is the current working directory.
    - _file_descriptor (str): A descriptor for the weather data file. Default is 'weather'.
    - _filename (str): The full filename for the weather data file.
    - _data (pd.DataFrame): A dataframe containing the stored weather data.
    - _dates (set): A list of available dates for the stored weather data.
    - _timezone (str): The timezone to use for the weather data. Default is 'Europe/Madrid'.

    Methods:
    - __init__(self, station:str = None, descriptor: str = None, basedir: str = None, tz: str = None): Initializes the class with the given parameters.
    - load(self)->pd.DataFrame: Loads the cached weather data from a csv file.
    - save(self): Saves the weather data to a csv file.
    - compose(self, dates:list=None, save:bool = True): Composes weather data for the given dates and adds it to the existing data.
    """

    weather_station = 'UK'
    weather_variables = pd.DataFrame({
        'code': [32, 33, 34, 35],
        'nom' : ['Temperatura', 'Humitat relativa', 'Pressió atmosfèrica', 'Precipitació'],
        'name': ['Temperature', 'Relative humidity', 'Air pressure', 'Precipitation'],
        'unit': ['ºC', '%', 'hPa', 'mm'],
        'acronym': ['T', 'RH', 'p', 'pp'],
        'decimals': [1,0,1,1]
    })
    _base_dir = None
    _file_descriptor = "weather"
    _filename = None
    _data = pd.DataFrame()
    _dates = set()
    _timezone = 'Europe/Madrid'
    
    def __init__(self, station:str = None, descriptor: str = None, basedir: str = None, tz: str = None):
        """
        Initializes the class with the given parameters.

        Args:
        - station (str): The code for the weather station to use.
        - descriptor (str): A descriptor for the weather data file.
        - basedir (str): The base directory for storing the weather data.
        - tz (str): The timezone to use for the weather data.
        """
        super().__init__()
        if station:
            self.weather_station = station
        if descriptor:
            self._file_descriptor = descriptor
        if basedir:
            self._base_dir = basedir
        if tz:
            self._timezone = tz

    def load(self) -> pd.DataFrame:
        """
        Loads the cached weather data from a csv file.

        Returns:
        - pd.DataFrame: The cached weather data.
        """
        pass

    def save(self):
        """
        Saves the weather data to a csv file.
        """
        pass

    def compose(self, dates: list = None, save: bool = True):
        """
        Composes weather data for the given dates and adds it to the existing data.

        Args:
        - dates (list): The dates to compose weather data for.
        - save (bool): Whether to save the weather data to a file.
        """
        pass

    def __init__(self, station:str = None, descriptor: str = None, basedir: str = None, tz: str = None):
        """
        Initializes the class with the given parameters.

        Args:
        - station (str): The code for the weather station to use. Default is 'YR'.
        - descriptor (str): A descriptor for the weather data file. Default is 'weather'.
        - basedir (str): The base directory for storing the weather data. Default is the current working directory.
        - tz (str): The timezone to use for the weather data. Default is 'Europe/Madrid'.

        Returns:
        - None

        Raises:
        - None
        """
        if station:
            self.weather_station = station
        if not basedir:
            self._base_dir = os.getcwd()
        else:
            self._base_dir = basedir
        if descriptor:
            self._file_descriptor = descriptor
        if tz:
            self._timezone = tz
        self._filename = f"{self._file_descriptor}_{self.weather_station}.csv"
        super().__init__()
        
    def load(self)->pd.DataFrame:
        """
        Loads the cached weather data from a csv file.

        Args:
        - None

        Returns:
        - pd.DataFrame: A dataframe containing the loaded weather data, or None if the file does not exist.

        Raises:
        - None
        """

        filename=Path(self._base_dir).joinpath(self._filename)
        # If the file doesn't exist yet, return None
        if not filename.is_file():
            return None
        self._data = pd.read_csv(filename)
        self._data['time'] = pd.to_datetime(self._data['time'])
        self._data['time'].dt.tz_convert(self._timezone)
        self._dates = set(self._data['time'].apply(lambda x: x.strftime(format='%Y-%m-%d')))

    def save(self):
        """
        Saves the weather data to a csv file.

        Args:
        - None

        Returns:
        - str: The filename of the saved file.

        Raises:
        - None
        """
        filename=Path(self._base_dir).joinpath(self._filename)
        return self._data.to_csv(filename, index=False)
    
    def compose(self, dates:list=None, save:bool = True):
        """
        Composes weather data for the given dates and adds it to the existing data.

        Args:
        - dates (list): A list of dates in the format 'YYYY-MM-DD' for which to compose weather data. If None, all available dates will be used.
        - save (bool): Whether to save the updated data to disk. Default is True.

        Returns:
        - None

        Raises:
        - None
        """
        # Check that the cache is loaded
        if not self._dates:
            self.load()

        # Now select those dates that are not in the list of cached data
        dates = list(set(dates) - set(self._dates))
        if not dates:
            return

        weather_data=pd.DataFrame()
        for date in dates:
            dp = date.split("-")
            day_df=pd.DataFrame({'hour': range(0,24)})
            for code in self.weather_variables.code:
                acronym = self.weather_variables.loc[self.weather_variables.code==code,'acronym'].iloc[0]
                url = f"/variables/mesurades/{code}/{dp[0]}/{dp[1]}/{dp[2]}?codiEstacio={self.weather_station}"
                try:
                    data = self.get_data(url,return_json=True)
                except ValueError:
                    print("No data for variable {acronym} on day {date}")
                    continue    
                # Now discard values that are not validated
                data = [item for item in data['lectures'] if item['estat']=='V']
                data = pd.json_normalize(data)
                data.drop(columns=['estat','baseHoraria'], inplace=True)
                data.rename(columns={'data':'time',
                                     'valor': acronym},
                                     inplace=True)
                data['time'] = pd.to_datetime(data['time'])
                data['time'].dt.tz_convert(self._timezone)
                data['hour'] = data['time'].apply(lambda x: x.hour)
                # And summarise by the hour
                res = data.groupby('hour').agg({'time': 'first', acronym: 'mean'})
                day_df = day_df.join(res, on=['hour'], how='left',rsuffix='_tmp')
                drop_cols = [col for col in day_df.columns if col.endswith('_tmp')]
                day_df.drop(columns=drop_cols, inplace=True)
            # Now we have a dataframe with the different variables summarised by the hour for one day
            # Add that to the resulting data frame
            weather_data = pd.concat([weather_data,day_df])

        weather_data.reset_index().drop(columns=['index'], inplace=True)
        weather_data['date'] = weather_data['time'].apply(lambda x: x.strftime(format='%Y-%m-%d'))
        # Shuffle around the columns
        new_order = ['time','date','hour']+list(set(weather_data.columns) - set(['time','date','hour']))
        weather_data = weather_data[new_order]
        # Now add this to the full data set and sort it by time
        self._data = pd.concat([self._data, weather_data])
        self._data.sort_values(by='time', ascending=True, inplace=True)
        # Reset the dates for later update calls
        self._dates = set(self._data['time'].apply(lambda x: x.strftime(format='%Y-%m-%d')))
        if save:
            self.save()

    def get_daily_weather(self, dates: list=None, startHour:int=None, endHour:int=None, fun: Callable=None)->pd.DataFrame:
            """
            Returns daily weather data for the specified dates, hours, and function.

            Args:
                dates (list, optional): List of dates in the format '%Y-%m-%d'. Defaults to [datetime.today().strftime("%Y-%m-%d")].
                startHour (int, optional): Starting hour for the data. Defaults to None.
                endHour (int, optional): Ending hour for the data. Defaults to None.
                fun (Callable, optional): Function to apply to the data. Defaults to None.

            Returns:
                pd.DataFrame: DataFrame containing the daily weather data.
            """
            if not dates:
                dates = [datetime.today().strftime("%Y-%m-%d")]
            # Download the data if necessary
            self.compose(dates)
            # Get the data for the selection of days
            df = self._data[self._data.date.isin(dates)].copy()
            if startHour:
                df = df.query(f'hour>={startHour}')
            if endHour:
                df = df.query(f'hour<={endHour}')
            # Transform if requested
            if  fun:
                df =  df.groupby('date').apply(fun)            
            return df
