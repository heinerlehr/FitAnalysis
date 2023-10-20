# -*- coding: utf-8 -*-
import keyring


class APIKey:
    """Provides a simple wrapper around the keystore.

    Can be instatiated or used via static methods.
    """
    def __init__(self, client: str):
        """Initialises the object and obtains the key from the system keystore

        Args:
            client (str): key in the keystore
        """        
        self._client = client
        self.key = keyring.get_password("system", self._client)

    def get(self):
        """Gets the password

        Returns:
            str: the passowrd
        """        
        return self.key

    def set(self, key: str):
        """Sets the password

        Args:
            key (str): the password to be set (or overwritten)
        """        
        keyring.set_password("system", self._client, key)
        self.key = key

    @staticmethod
    def getKeyForClient(client: str):
        """Static method to obtain the password for a specific key 

        Args:
            client (str): The key

        Returns:
            str: The password
        """        
        return keyring.get_password("system", client)
