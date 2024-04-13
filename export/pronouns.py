# Copyright (c) 2022-2024 Linh Pham
# wwdtm_database_export is released under the terms of the Apache License 2.0
# SPDX-License-Identifier: Apache-2.0
#
# vim: set noai syntax=python ts=4 sw=4:
"""Preferred Pronouns Database Export Module."""
import json
from typing import Any

from mysql.connector import connect
from mysql.connector.connection import MySQLConnection
from mysql.connector.pooling import PooledMySQLConnection


class Pronouns:
    """Wait Wait Stats Database Preferred Pronouns.

    This class contains database methods used to export preferred
    pronouns data from a copy of the Wait Wait Stats database.

    :param connect_dict: Dictionary containing database connection
        settings as required by mysql.connector.connect
    :param database_connection: mysql.connector.connect database
        connection
    """

    def __init__(
        self,
        connect_dict: dict[str, Any] | None = None,
        database_connection: MySQLConnection | PooledMySQLConnection | None = None,
    ):
        """Class initialization method."""
        if connect_dict:
            self.connect_dict = connect_dict
            self.database_connection = connect(**connect_dict)
        elif database_connection:
            if not database_connection.is_connected():
                database_connection.reconnect()

            self.database_connection = database_connection

    def to_json(self) -> str:
        """Returns database contents of ww_pronouns as JSON.

        :return: Contents of the ww_pronouns table as JSON
        """
        cursor = self.database_connection.cursor(dictionary=True)
        query = """
            SELECT pronounsid, pronouns FROM ww_pronouns
            ORDER BY pronounsid ASC;
            """
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        if results:
            return json.dumps(results, indent=2, sort_keys=False)