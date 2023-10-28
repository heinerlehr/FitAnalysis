# -*- coding: utf-8 -*-
# pylint disable=keyword-arg-before-var-arg 

import pandas as pd
import zipfile
from datetime import datetime, date, tzinfo
import pytz
from pathlib import Path
import os
from typing import Union, List, Callable
import re

class OverViewFitMetrics:
    '''Class that will hold the overview of daily metrics from a Google Fit Takeout
    '''

    # Holds the location of the directory or (zipped) file of the takeout
    _basedir_or_file = ''

    # Holds the raw data from the daily overview
    _fit_overview=pd.DataFrame()

    # Structure where the the overview file is located
    _overview_file = 'Takeout/Fit/Daily activity metrics/Daily activity metrics.csv'

    # Column shortnames
    _column_shortnames={
        'min':'Move Minutes count', 
        'cal':'Calories (kcal)', 
        'dis':'Distance (m)', 
        'hp':'Heart Points',
        'hm':'Heart Minutes', 
        'hr':'Average heart rate (bpm)',
        'maxhr':'Max heart rate (bpm)',
        'minhr':'Min heart rate (bpm)', 
        'lowlat':'Low latitude (deg)',
        'lowlong':'Low longitude (deg)',
        'hilat':'High latitude (deg)',
        'hilong':'High longitude (deg)',
        'avgspeed':'Average speed (m/s)',
        'maxspeed':'Max speed (m/s)',
        'minspeed':'Min speed (m/s)',
        'sc':'Step count',
        'avgweight':'Average weight (kg)',
        'maxweight':'Max weight (kg)',
        'minweight':'Min weight (kg)',
        'cycling':'Cycling duration (ms)',
        'inactive':'Inactive duration (ms)',
        'unknown':'Unknown duration (ms)',
        'walking':'Walking duration (ms)',
        'running':'Running duration (ms)',
        'rowing':'Rowing machine duration (ms)',
        'treadmill':'Treadmill running duration (ms)',
        'stairs':'Stair climbing duration (ms)',
        'treadmillwalking':'Treadmill duration (ms)'
        }
    # The flipped mapping
    _column_longnames={}

     # Activity types
    _activity_types = (
        'cycling',
        'inactive',
        'unknown',
        'walking',
        'running',
        'rowing_machine',
        'treadmill',
        'stairs',
        'treadmillwalking'
    )

    # Time groupings
    _time_grouping = ('Day','Weekday','Week','Month','Year')

    def __init__ (self, dir_or_file: str):
        """Initiate class with reference to the basefile

        Args:
            dir_or_file (str): The file or dir where the takeout is stored
        """
        self._basedir_or_file = Path(dir_or_file)
        if not self._basedir_or_file.is_file():
            raise FileNotFoundError
        extension = os.path.splitext(dir_or_file)[1]
        if 'zip' in extension:
            self.load_from_zip(self._basedir_or_file)
        else:
            self.load_from_csv(self._basedir_or_file)
        # After loading, do some housekeeping
        self.prepare_data_overview()

    def load_from_zip(self, file: str):
        """Loads the overview data from a zipped takeout file

        Args:
            file (str): the name and location of the zip file
        """
        with zipfile.ZipFile(file, 'r') as zipped:
            zipped.extract(self._overview_file)
            self._fit_overview = pd.read_csv(self._overview_file)
    
    def load_from_csv(self, dir: str):
        """Loads the overview data from an unzipped takeout file

        Args:
            dir (str): the path of the directory holding the takeout
        """
        overview_filename = dir+"/"+self._overview_file
        overview_file=Path(overview_filename)
        if not overview_file.is_file():
            raise FileNotFoundError
        self._fit_overview = pd.read_csv(overview_file)

    def prepare_data_overview(self):
        """Prepare the raw data for analysis.
        1. Convert all columns with "date" in the name to datetime
        2. Set data as the index
        3. Rename columns
        """

        # Convert "Date" column to datetime data type and set it as the index
        self._fit_overview['Date'] = pd.to_datetime(self._fit_overview['Date'])
        self._fit_overview.set_index('Date', inplace=True)

        # Convert all remaining date columns to datetime data type
        date_columns = [col for col in self._fit_overview.columns if 'date' in col.lower()]
        self._fit_overview[date_columns] = self._fit_overview[date_columns].apply(pd.to_datetime)

        # Now rename the columns for easier access
        self._column_longnames = {value:key for key, value in self._column_shortnames.items()}
        self._fit_overview.rename(columns=self._column_longnames, inplace=True)

        # Add helper columns
        self._fit_overview['Weekday']=self._fit_overview.index.isocalendar().day
        self._fit_overview['Week']=self._fit_overview.index.isocalendar().week
        self._fit_overview['Month']=self._fit_overview.index.month
        self._fit_overview['Year']=self._fit_overview.index.year
        
    @staticmethod
    def get_columns()->dict:
        """Access method for column name mapping short:long

        Returns:
            dict: Dictionary of short column names: original column names
        """
        return OverViewFitMetrics._column_shortnames
    
    @staticmethod
    def get_activity_types()->dict:
        return OverViewFitMetrics._activity_types
    
    def get_data(self)->pd.DataFrame:
        """Return a copy of the raw data set. Given that we don't want surprises, we'll return a shallow copy

        Returns:
            pd.DataFrame: A shallow copy of the data contained in the overview
        """
        return self._fit_overview.copy()
    
    def _find_column_name(self, column: str)->str:
        """Finds the column name from a short or long variable name

        Args:
            column (str): short or long variable name

        Raises:
            KeyError: when there is no such short or long variable name

        Returns:
            str: the column name
        """
        if not isinstance(column,str):
            raise ValueError(f"The column name {column} needs to be a string. You provided {type(column)}")
        if column not in self._column_shortnames.keys():
            if not column in self._column_longnames.keys():
                raise KeyError(f"No such variable name: {column}")
            else:
                return self._column_longnames[column]        
        else:
            return column

    def _find_column_names(self, columns: Union[str, List[str]])->Union[str,list[str]]:
        """Take a list of long or short column names and returns a list of short column names

        Args:
            columns (str or list): A column or a list of columns

        Returns:
            list or str: Name of the column or list of column names 
        """
        if isinstance(columns, str):
            # If it's a string, process it as a single item
            return self._find_column_name(columns)
        elif isinstance(columns, list):
            # If it's a list, process each item separately
            return [self._find_column_name(item) for item in columns]

    def get_variables_per_day(self, vars: Union[str, List[str]],dropna:bool = True)->pd.DataFrame:
        """Obtains one of the variables in the file per day.

        Args:
            vars (Union[str, List[str]]): Is the name or list of variables, either in the abbreviated or the long format

        Returns:
            pd.DataFrame: pd.DataFrame with date as index and the selected columns. Drops NaN values.
        """
        vars = self._find_column_names(vars)
        if dropna:
            return self._fit_overview[vars].dropna()
        else:
            return self._fit_overview[vars]
    
    def get_variables_per_timeframe(self, vars: Union[str, List[str]], timeframe:str ='Day',dropna:bool = True, fun:Callable = pd.DataFrame.median)->pd.DataFrame:
        """Obtains a summary of one or several variables for a specific timeframe

        Args:
            vars (Union[str, List[str]]): the variable(s)
            timeframe (str, optional): The timeframe, i.e. one of 'Day','Weekday','Week','Month','Year'. Defaults to 'Day'.
            fun (Callable, optional): The summary function. Defaults to pd.DataFrame.median.

        Raises:
            ValueError: if incorrect timeframe is provided

        Returns:
            pd.DataFrame: DataFrame with summarised data
        """
        if timeframe == 'Day':
            return self.get_variables_per_day(vars)
        elif timeframe not in self._time_grouping:
            raise ValueError(f"Invalid time grouping {timeframe}. Available values are {self._time_grouping}")
        
        vars = self._find_column_names(vars)
        df = self._fit_overview.groupby(timeframe).apply(fun)
        if dropna:
            return df[vars].dropna()
        else:
            return df[vars]
