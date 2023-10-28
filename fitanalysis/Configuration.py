# -*- coding: utf-8 -*-
"""
Reads configuration file and provides values to rest of the applicaton. Configuration files are in yaml.
NOTE: all .yaml files in the path will be read and processed. The path is currently set as:

Given that the configuration files contain language dependent labels, this is also the place where
the language processor is installed.

In order to access individual parameters from the yaml parameter hierarchy the instance can be used as a function
cfg=Configuration(Client)
ParamValue1=cfg("Param1")
ParamValue2=cfg("ParamGroup","Param2")
ParamValue3=cfg("ParamGroup","ParamSubGroup","Param3")

@author: HL
"""
import os
from pathlib import Path
from himl import ConfigProcessor

class Configuration(object):

    # Holds the configuration tree as read from the yaml file
    _cfg = None

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Configuration, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        if self._cfg is not None:
            return
        basedir = self.get_base_dir()

        config_processor = ConfigProcessor()

        filters = ()  # can choose to output only specific keys
        exclude_keys = ()  # can choose to remove specific keys
        output_format = "yaml"  # yaml/json

        if not Path(basedir).exists():
            raise ValueError(f"No such path {basedir}")  # If that path doesn't exist either, it's best to throw an exception
        self._cfg = config_processor.process(path=basedir, filters=filters, exclude_keys=exclude_keys, output_format=output_format, print_data=False)

    def __call__(self, *params):
        try:
            if len(params) == 1:
                return self._cfg[params[0]]
            elif len(params) == 2:
                return self._cfg[params[0]][params[1]]
            else:
                return self._cfg[params[0]][params[1]][params[2]]
        except KeyError:
            return None
            # raise KeyError(f"**** ERROR: Configuration file doesn't contain {params}.")
    
    def get_base_dir(self):
        basedir = os.path.dirname(os.path.dirname(__file__))
        return basedir

    @staticmethod
    def get_config():
        return Configuration()