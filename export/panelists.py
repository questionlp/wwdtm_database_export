# -*- coding: utf-8 -*-
# vim: set noai syntax=python ts=4 sw=4:
#
# Copyright (c) 2022-2023 Linh Pham
# wwdtm_database_export is released under the terms of the Apache License 2.0
"""Panelists Database Export Module"""

import json
from typing import Any, Dict, Optional

from mysql.connector import connect


class Panelists:
    """This class contains database methods used to export panelists
    data from a copy of the Wait Wait Stats database.

    :param connect_dict: Dictionary containing database connection
        settings as required by mysql.connector.connect
    :param database_connection: mysql.connector.connect database
        connection
    """

    def __init__(
        self,
        connect_dict: Optional[Dict[str, Any]] = None,
        database_connection: Optional[connect] = None,
    ):
        """Class initialization method"""
        if connect_dict:
            self.connect_dict = connect_dict
            self.database_connection = connect(**connect_dict)
        elif database_connection:
            if not database_connection.is_connected():
                database_connection.reconnect()

            self.database_connection = database_connection

    def to_json(self) -> str:
        """Returns the contents of the ww_panelists database table as a
        JSON string.

        :return: Contents of the ww_panelists table as JSON
        """
        cursor = self.database_connection.cursor(dictionary=True)
        query = """
            SELECT panelistid, panelist, panelistgender, panelistslug
            FROM ww_panelists
            ORDER BY panelistid ASC;
            """
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        if results:
            return json.dumps(results, indent=2, sort_keys=False)
