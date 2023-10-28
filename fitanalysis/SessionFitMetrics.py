class SessionFitMetrics:
    """Class that will hold the metrics of session of sessions from a Google Fit Takeout 

    Attributes:
        _basedir_or_file (str): Holds the location of the directory or (zipped) file of the takeout
        _fit_session_data (pandas.DataFrame): Holds the raw data from the daily overview
        _session_folder (str): Structure where the the overview file is located
        _ref_dates (list): Holds the reference date (contained in the file name)
        _load_files (list): Holds the filename
        _column_shortnames (dict): The mapping of short column names to long column names
        _column_longnames (dict): The flipped mapping
        _activities (list): Activities we are interested in
        _time_grouping (tuple): Time groupings
        _local_tz (pytz.tzfile): The timezone that calculations will be done in
        _label (str): The center point for the timestamp. Must be "Left", "Right" or "Center"
        _label_options (tuple): The allowable center point options

    Methods:
        __init__: Creates an instance of DailyFitMetrics that will hold the Google Fit data of one day taken from a takeout file
        load_from_zip: Loads the overview data from a zipped takeout file
        load_from_dir: Loads the overview data from an unzipped takeout file
        _find_files: Finds all activity files that correspond to the dates
        _extract_dates: Extracts a list of dates in the correct format
        prepare_data_overview: After loading, does some housekeeping
        get_data: Returns the data for the given date
        get_activity_types: Returns the list of activity types
        get_time_grouping: Returns the time grouping
        set_time_grouping: Sets the time grouping
        get_activities: Returns the list of activities
        set_activities: Sets the list of activities
        get_local_tz: Returns the local timezone
        set_local_tz: Sets the local timezone
        get_label: Returns the center point for the timestamp
        set_label: Sets the center point for the timestamp
        get_column_names: Returns the column names
        get_data_by_activity: Returns the data for the given activity
        get_data_by_time: Returns the data for the given time
        get_data_by_activity_and_time: Returns the data for the given activity and time
        get_summary: Returns a summary of the data
    """
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
import json
import fitanalysis as gfit

class SessionFitMetrics:
    """Class that will hold the metrics of a particular day from a Google Fit Takeout 
    """
    # Holds the location of the directory or (zipped) file of the takeout
    _basedir_or_file = ''

    # Holds the raw data from the daily overview
    _fit_session_data=pd.DataFrame()

    # Structure where the the overview file is located
    _session_folder = 'Takeout/Fit/All sessions/'

    # Holds the reference date (contained in the file name)
    _ref_dates = list()

    # Holds the filename
    _load_files = list()
    
    # The mapping of short column names to long column names
    _column_shortnames={}

    # The flipped mapping
    _column_longnames={'com.google.active_minutes':'amin',
                       'com.google.calories.expended':'cal',
                       'com.google.distance.delta':'dis',
                       'com.google.heart_minutes.summary':'hm',
                       'com.google.speed.summary':'avgspeed',
                       'com.google.step_count.delta': 'sc',
                       'fitnessActivity':'act',
                       'startTime':'start',
                       'endTime':'end',
                       'duration':'min',
                       }
    
    # Activities we are interested in
    _activities = list()
    # Time groupings
    _time_grouping = ('Day','Hour','Asis')
    # The timezone that calculations will be done in
    _local_tz = pytz.timezone('Europe/Madrid')
    # The center point for the timestamp. Must be "Left", "Right" or "Center"
    _label = ""
    _label_options = ("Left", "Right", "Center")
    
    def __init__ (self, dir_or_file:str, dates:list, activities:list = None, tz:str="Europe/Madrid", label:str = "Center"):
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

        # Ensure activities are in the list of activities
        if activities and not set(activities).issubset(gfit.OverViewFitMetrics.get_activity_types()):
            raise ValueError(f"Activities must be a subset of {gfit.OverViewFitMetrics.get_activity_types()} or empty. Got {activities}.")

        # Set the desired timezone
        self._local_tz=pytz.timezone(tz)
        # Obtain a list of dates in the correct format
        self._ref_dates = self._extract_dates(dates)
        # Flip the column names for easier access
        self._column_shortnames = {value:key for key, value in self._column_longnames.items()}
        # Find all activity files that correspond to the dates
        is_zip,self._load_files = self._find_files(dir_or_file, self._ref_dates, activities)
        # After loading, do some housekeeping
        self.prepare_data_overview(is_zip, dir_or_file)

    def load_from_zip(self, dir_or_file: str, file: str):
        """Loads the overview data from a zipped takeout file

        Args:
            file (str): the name and location of the zip file
        """
        with zipfile.ZipFile(dir_or_file, 'r') as zipped:
            zipped.extract(file)
            with open(file) as json_file:
                data = json.load(json_file)
        return data
    
    def load_from_dir(self, dir_or_file: str, file: str):
        """Loads the overview data from an unzipped takeout file

        Args:
            dir (str): the path of the directory holding the takeout
        """
        session_file=Path(os.path.join(dir_or_file, self._session_folder,file))
        with open(session_file) as json_file:
            data = json.load(json_file)
        return data

    def _find_files(self, dir_or_file: str, dates:list, activities: list)->list:
            """
            Finds files in a directory or zip file based on specified dates and activities.

            Args:
            dir_or_file (str): The directory or zip file to search for files.
            dates (list): A list of dates in the format 'YYYY-MM-DD' to search for files.
            activities (list): A list of activity names to search for files.

            Returns:
            tuple: A tuple containing a boolean indicating whether the input was a zip file, and a list of selected file names.
            """
            # Check whether the takeout is zipped or not
            self._basedir_or_file = Path(dir_or_file)
            if not self._basedir_or_file.exists():
                raise FileNotFoundError
            
            extension = os.path.splitext(dir_or_file)[1]
            if 'zip' in extension:
                is_zip = True
                files = zipfile.ZipFile(dir_or_file).namelist()
                selected_files = [f for f in files 
                                 if f.startswith(self._session_folder) and 
                                 f[len(self._session_folder):len(self._session_folder)+10] in dates]
                if activities:
                    selected_files = [f for f in selected_files if f.split('_')[-1].lower().split('.')[0] in activities]
            else:
                is_zip = False
                files = os.listdir(os.path.join(dir_or_file,self._session_folder))
                selected_files = [f for f in files if f[0:len('YYYY-MM-DD')] in dates]
                if activities:
                    selected_files = [f for f in selected_files if f.split('_')[-1].lower().split('.')[0] in activities]
            return is_zip, selected_files

    def _extract_dates(self, dates:list)->list:
            """
            Extracts dates from a list of dates in string or date format.
            If the date is in string format, it tries to convert it to date format.
            If the date is already in date format, it converts it to string format.
            If the date is not in a recognized format, it raises a ValueError.
            
            Args:
            - dates: A list of dates in string or date format.
            
            Returns:
            - A list of dates in string format (YYYY-MM-DD).
            """
            ret_dates = []
            for load_date in dates:
                if isinstance(load_date,str):
                    # Convert to date string
                    # Try DD-MM-YYYY first
                    ddmmyyyy_regex = re.compile(r'([0-9]{4})[-./]([0-9]{2})[-./]([0-9]{2})')
                    date_mo = ddmmyyyy_regex.search(load_date)
                    if date_mo:
                        # Test whether this is a valid date
                        self._ref_date = date(year=int(date_mo.group(1)), month=int(date_mo.group(2)), day=int(date_mo.group(3)))
                        # OK that didn't seem to raise an error, so we're done
                        ret_dates.append(date_mo.group(1)+"-"+date_mo.group(2)+"-"+date_mo.group(3))
                    else: # OK, so it must be in DD-MM-YYYY format
                        ddmmyyyy_regex = re.compile(r'([0-9]{2})[-./]([0-9]{2})[-./]([0-9]{4})')
                        date_mo = ddmmyyyy_regex.search(load_date)
                        if not date_mo:
                            # Giving up, this is not a format I understand
                            raise ValueError(f"Date is in incorrect format: {load_date}. Please use YYYY-MM-DD or DD-MM-YYYY.")
                        # Test whether this is a valid date
                        self._ref_date = date(year=int(date_mo.group(3)), month=int(date_mo.group(2)), day=int(date_mo.group(1)))
                        # OK that didn't seem to raise an error, so we're done
                        ret_dates.append(date_mo.group(3)+"-"+date_mo.group(2)+"-"+date_mo.group(1))
                elif isinstance(load_date,date):
                    # Convert to date string
                    ret_dates.append(load_date.strftime("%Y-%m-%d"))
                else:
                    gfit.logmsg(f"Date is in incorrect format: {load_date}. Please use YYYY-MM-DD or DD-MM-YYYY.",'INFO')
            return ret_dates
    
    def _extractFromAggregate(self, aggregates: list, df:pd.DataFrame):
        """Extracts the values from 'aggregate' part of a Session file and adds 
        desired values as columns to the dataframe

        Args:
            aggregates (list): 'aggregate' part of a Session file
            df (pd.DataFrame): a dataframe to add to 
        """

        for aggregate in aggregates:
            if 'floatValue' in aggregate.keys():
                value = float(aggregate['floatValue'])
            elif 'intValue' in aggregate.keys():
                value = float(aggregate['intValue'])
            else:
                continue
            if aggregate['metricName'] in self._column_longnames.keys():
                df.loc[0,self._column_longnames[aggregate['metricName']]] = value

    def extract_activities_from_json(self, data: dict)->pd.DataFrame:
        """Extracts the desireed values from the contents of a Session file and returns them as a dataframe of one row

        Args:
            data (dict): contents of a Session file in json

        Returns:
            pd.DataFrame: A 1 row dataframe with the desired values in the columns as described by the values in self._column_shortnames
        """     
        df = pd.DataFrame({
            'act': [str(data[self._column_shortnames['act']])],
            'start' : [pd.to_datetime(datetime.fromisoformat(data[self._column_shortnames['start']]).astimezone(self._local_tz))],
            'end' : [pd.to_datetime(datetime.fromisoformat(data[self._column_shortnames['end']]).astimezone(self._local_tz))],
            'min' : [float(data[self._column_shortnames['min']].replace('s',''))]
        })
        self._extractFromAggregate(data['aggregate'],df)
        return df  

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
        
    def prepare_data_overview(self, is_zip:bool = True, dir_or_file:str = ''):
        """Prepare the raw data for analysis.
        1. For all self._load_files load the file (from zip or from directory)
        2. Extract one line of data from each file and concatenate to a dataframe
        3. Store in self._fit_session_data
        """
        for file in self._load_files:
            if is_zip:
                data = self.load_from_zip(dir_or_file, file)
            else:
                data = self.load_from_dir(dir_or_file, file)
            # Extract the data from the file contents
            df = self.extract_activities_from_json(data)
            # Add the data to the main dataframe
            self._fit_session_data = pd.concat([self._fit_session_data,df], ignore_index=True) 
        
        # Convert "Date" column to datetime data type and set it as the index
        self._fit_session_data['Date'] = self._fit_session_data['start'].dt.date
        # Add helper columns
        self._fit_session_data['Weekday']=[row['start'].day for index, row in self._fit_session_data.iterrows()]
        self._fit_session_data['Week']=[row['start'].week for index, row in self._fit_session_data.iterrows()]
        self._fit_session_data['Month']=[row['start'].month for index, row in self._fit_session_data.iterrows()]
        self._fit_session_data['Year']=[row['start'].year for index, row in self._fit_session_data.iterrows()] 
    
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
            
    def get_variables_per_time(self, vars: Union[str, List[str]], timeframe:str ='Day', label:str = 'Left', dropna:bool = True, fun:Callable = pd.DataFrame.median):
            """
            Returns a DataFrame with the specified variables aggregated by the specified timeframe.

            Args:
            - vars (Union[str, List[str]]): A string or list of strings representing the variable(s) to be aggregated.
            - timeframe (str): A string representing the timeframe to aggregate the variables by. Must be one of ['Day', 'Hour', 'Asis'].
            - label (str): A string representing the label to use for the returned DataFrame.
            - dropna (bool): A boolean indicating whether to drop rows with missing values.
            - fun (Callable): A callable function to apply to the grouped data. Default is pd.DataFrame.median.

            Returns:
            - df (DataFrame): A DataFrame with the specified variables aggregated by the specified timeframe.
            """
            t_vars = self._find_column_names(vars)
            # Since _find_column_names can return a list or a str append is used


            df = self._fit_session_data.copy()
            match timeframe:
                case 'Day':
                    # There is only one line to be returned because the whole file is for one day
                    df = df.drop(columns=['act','start','end']).groupby(['Date', 'Weekday','Week','Month','Year']).apply(fun)
                    df = df[vars]
                case 'Hour': # Hour
                    # Create a column hour from the centerpoint and group by it
                    df['hour'] = df['start'].apply(lambda x: x.hour)             
                    df = df.drop(columns=['act','start','end','Date', 'Weekday','Week','Month','Year']).groupby(['hour']).apply(fun)
                    df = df[vars]
                case 'Asis': # Quarter
                    # The data just as it is; not likely to be useful
                    df = df[vars]
                case _:                      # Other
                    raise ValueError(f"timeframe must be on of {self._time_grouping}.")
            return df
