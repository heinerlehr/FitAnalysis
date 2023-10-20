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
from fitanalysis.OverViewFitMetrics import OverViewFitMetrics

class DailyFitMetrics:
    """Class that will hold the metrics of a particular day from a Google Fit Takeout 
    """
    # Holds the location of the directory or (zipped) file of the takeout
    _basedir_or_file = ''

    # Holds the raw data from the daily overview
    _fit_day_data=pd.DataFrame()

    # Structure where the the overview file is located
    _day_folder = 'Takeout/Fit/Daily activity metrics/'

    # Holds the reference date (contained in the file name)
    _ref_date = date.today()

    # Holds the filename
    _load_file = ""
    
    # The mapping of short column names to long column names
    _column_shortnames={}
    # The flipped mapping
    _column_longnames={}
    # Time groupings
    _time_grouping = ('Day','Hour','Quarter')
    # The timezone that calculations will be done in
    _local_tz = pytz.timezone('Europe/Madrid')
    # The center point for the timestamp. Must be "Left", "Right" or "Center"
    _label = ""
    _label_options = ("Left", "Right", "Center")
    
    def __init__ (self, load_date:Union[str, date], dir_or_file:str, tz:str="Europe/Madrid", label:str = "Center"):
        """Creates an instance of DailyFitMetrics that will hold the Google Fit data of one day taken from a takeout file

        Args:
            load_date (str or date): The date for which the data is to be loaded. Format can be "YYYY-MM-DD" or "DD-MM-YYYY". Allowable separators are '-','.' and '/'
            dir_or_file (str): The zipped takeout file or the base directory of the unzipped tree
            tz (str, optional): A clear text timezone. Defaults to "Europe/Madrid".
            label (str, optional): How to determine the centerpoint between Start and End time. Defaults to "Center".

        Raises:
            ValueError: For label not in Left|Right|Center
            ValueError: For load_date not being either a string or a date
            ValueError: For load_date being incorrectly formatted
            FileNotFoundError: If there is no data for the requested date (or no takeout file/dir)
        """
        if label not in self._label_options:
            raise ValueError(f"Wrong label: {label}. Has to be one of {self._label_options}")
        self._label = label
        if isinstance(load_date,str):
            # Convert to date string
            # Try DD-MM-YYYY first
            ddmmyyyy_regex = re.compile(r'([0-9]{4})[-./]([0-9]{2})[-./]([0-9]{2})')
            date_mo = ddmmyyyy_regex.search(load_date)
            if date_mo:
                # Test whether this is a valid date
                self._ref_date = date(year=int(date_mo.group(1)), month=int(date_mo.group(2)), day=int(date_mo.group(3)))
                # OK that didn't seem to raise an error, so we're done
                load_file = date_mo.group(1)+"-"+date_mo.group(2)+"-"+date_mo.group(3)
            else: # OK, so it must be in DD-MM-YYYY format
                ddmmyyyy_regex = re.compile(r'([0-9]{2})[-./]([0-9]{2})[-./]([0-9]{4})')
                date_mo = ddmmyyyy_regex.search(load_date)
                if not date_mo:
                    # Giving up, this is not a format I understand
                    raise ValueError(f"Date is in incorrect format: {load_date}. Please use YYYY-MM-DD or DD-MM-YYYY.")
                # Test whether this is a valid date
                self._ref_date = date(year=int(date_mo.group(3)), month=int(date_mo.group(2)), day=int(date_mo.group(1)))
                # OK that didn't seem to raise an error, so we're done
                load_file = date_mo.group(3)+"-"+date_mo.group(2)+"-"+date_mo.group(1)
        elif isinstance(load_date,date):
            # Convert to date string
            load_file = load_date.strftime("%Y-%m-%d")
            self._ref_date = load_date
        else:
            raise ValueError(f"Date is in incorrect format: {load_date}. Please use YYYY-MM-DD or DD-MM-YYYY.")
        
        # Now add the path and the extension
        self._load_file = self._day_folder+load_file+".csv"
        # Check whether the takeout is zipped or not
        self._basedir_or_file = Path(dir_or_file)
        if not self._basedir_or_file.is_file():
            raise FileNotFoundError
        
        extension = os.path.splitext(dir_or_file)[1]
        if 'zip' in extension:
            self.load_from_zip(self._basedir_or_file)
        else:
            self.load_from_csv(self._basedir_or_file)

        # Set the desired timezone
        self._local_tz=pytz.timezone(tz)
        # After loading, do some housekeeping
        self.prepare_data_overview()

    def load_from_zip(self, file: str):
        """Loads the overview data from a zipped takeout file

        Args:
            file (str): the name and location of the zip file
        """
        with zipfile.ZipFile(file, 'r') as zipped:
            zipped.extract(self._load_file)
            self._fit_day_data = pd.read_csv(self._load_file)
    
    def load_from_csv(self, dir: str):
        """Loads the overview data from an unzipped takeout file

        Args:
            dir (str): the path of the directory holding the takeout
        """
        overview_filename = dir+"/"+self._load_file
        overview_file=Path(overview_filename)
        if not overview_file.is_file():
            raise FileNotFoundError
        self._fit_day_data = pd.read_csv(overview_file)
    
    @staticmethod
    def _get_datetime(refdate:date, time:str, tz:tzinfo) -> datetime:
        """Helper method. Takes a date and a string time and returns a datetime

        Args:
            refdate (date): the reference date
            time (str):a string representing a time with reference timezone information
            tz (tzinfo): A timezone object

        Returns:
            datetime: The resulting date time in the chosen timezone
        """
        datetime_string = refdate.strftime("%Y%m%d")+" "+time
        return datetime.strptime(datetime_string,"%Y%m%d %H:%M:%S.%f%z").astimezone(tz)
        
    def prepare_data_overview(self):
        """Prepare the raw data for analysis.
        1. Create a proper start and end time with the date of the file
        2. Rename columns
        3. Set 'time' column to the chosen centerpoint (Left|Right|Center)
        """
        # Make Start time and End time datetimes with the date being the reference date
        self._fit_day_data['Start time'] = [DailyFitMetrics._get_datetime(self._ref_date,row['Start time'],self._local_tz) for index, row in self._fit_day_data.iterrows()]
        self._fit_day_data['End time']   = [DailyFitMetrics._get_datetime(self._ref_date,row['End time']  ,self._local_tz) for index, row in self._fit_day_data.iterrows()]
        # Copy the column name mapping from the OverviewFitMetrics class
        self._column_shortnames = OverViewFitMetrics.get_columns()
        self._column_shortnames.update({'st': 'Start time','et': 'End time'})
        # Now rename the columns for easier access
        self._column_longnames = {value:key for key, value in self._column_shortnames.items()}
        self._fit_day_data.rename(columns=self._column_longnames, inplace=True)
        # And finally set center point time according to the spec when the centrepoint should be
        self._fit_day_data['time'] = self._get_center_point(self._fit_day_data, self._label)
    
    def _find_column_name(self, column: str)->str:
        """Finds the column name from a short or long variable name

        Args:
            column (str): short or long variable name

        Raises:
            KeyError: when there is no such short or long variable name

        Returns:
            str: the column name
        """
        if not column in self._column_shortnames.keys():
            if not column in self._column_longnames.keys():
                raise KeyError(f"No such variable name: {column}")
            else:
                return self._column_longnames[column]        
        else:
            return column
        
    def _find_column_names(self, columns: Union[str, List[str]])->list:
        """Take a list of long or short column names and returns a list of short column names

        Args:
            columns (str or list): A column or a list of columns

        Returns:
            list: short column names
        """
        if isinstance(columns, str):
            # If it's a string, process it as a single item
            return self._find_column_name(columns)
        elif isinstance(columns, list):
            # If it's a list, process each item separately
            return [self._find_column_name(item) for item in columns]

    def _get_center_point(self, df: pd.DataFrame, label:str = 'Center')->list:
        """Calculates the center point between start time and end time according to one of Left|Right|Center

        Args:
            df (pd.DataFrame): a data frame with columns 'st' (start time) and 'et' (end time)
            label (str, optional): The manner how to calculate the center point. Defaults to 'Center'.

        Raises:
            ValueError: For an incorrect label

        Returns:
            list: Center point values
        """
        match label:
            case 'Left':
                return df['st']
            case 'Right':
                return df['et']
            case 'Center':
                return [ row['st']+(row['et']-row['st'])/2 for index,row in df.iterrows()]
            case _:
                raise ValueError(f"Incorrect label {label}. Must be one of {self._label_options}")
            
    def get_variables_per_time(self, vars: Union[str, List[str]], timeframe:str ='Quarter', label:str = 'Left', dropna:bool = True, fun:Callable = pd.DataFrame.median):
        t_vars = self._find_column_names(vars)
        # Since _find_column_names can return a list or a str append is used
        if isinstance(t_vars,str): 
            vars = ['st', 'et','time',t_vars]
        else:
            vars = ['st', 'et','time'] + t_vars

        df = self._fit_day_data.copy()
        match timeframe:
            case 'Day':
                # There is only one line to be returned because the whole file is for one day
                df = df.apply(fun)
                df = df[vars]
            case 'Hour': # Hour
                # Create a column hour from the centerpoint and group by it
                df = df[vars]
                df['hour'] = df['time'].apply(lambda x: x.hour)             
                df = df.groupby('hour').apply(fun)
            case 'Quarter': # Quarter
                # The data is already in quarter of an hour, so now the task is to create the "center point" in a new column "time"
                df = df[vars]
            case _:                      # Other
                raise ValueError(f"timeframe must be on of {self._time_grouping}.")
        return df
