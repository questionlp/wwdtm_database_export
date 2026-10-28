# Copyright (c) 2022-2026 Linh Pham
# wwdtm_database_export is released under the terms of the Apache License 2.0
# SPDX-License-Identifier: Apache-2.0
#
# vim: set noai syntax=python ts=4 sw=4:
"""Scorekeepers Tables Export Module."""

import json
from typing import Any

from mysql.connector import connect
from mysql.connector.connection import MySQLConnection
from mysql.connector.pooling import PooledMySQLConnection


class Scorekeepers:
    """Wait Wait Stats Database Scorekeepers.

    This class contains database methods used to export scorekeepers
    data from a copy of the Wait Wait Stats database.

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

    def to_json(self) -> str | None:
        """Returns database contents of ww_scorekeepers as JSON.

        :return: Contents of the ww_scorekeepers table as JSON
        """
        cursor = self.database_connection.cursor(dictionary=True)
        query = """
            SELECT scorekeeperid, scorekeeper, scorekeepergender, scorekeeperslug
            FROM ww_scorekeepers
            ORDER BY scorekeeperid ASC;
            """
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        if results:
            return json.dumps(results, indent=4, sort_keys=False)

    def external_sites_map_to_json(self) -> str | None:
        """Returns database contents of ww_skextsitesmap as JSON.

        :return: Contents of ww_skextsitesmap as JSON
        """
        cursor = self.database_connection.cursor(dictionary=True)
        query = """
            SELECT skextsitesmapid, scorekeeperid, siteid, url, account
            FROM ww_skextsitesmap
            ORDER BY skextsitesmapid ASC;
            """
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        if results:
            return json.dumps(results, indent=4, sort_keys=False)

    def pronouns_map_to_json(self) -> str | None:
        """Returns database contents of ww_skpronounsmap as JSON.

        :return: Contents of ww_skpronounsmap as JSON
        """
        cursor = self.database_connection.cursor(dictionary=True)
        query = """
            SELECT skpronounsmapid, scorekeeperid, pronounsid
            FROM ww_skpronounsmap
            ORDER BY skpronounsmapid ASC;
            """
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        if results:
            return json.dumps(results, indent=4, sort_keys=False)

    def social_media_accounts_map_to_json(self) -> str | None:
        """Returns database contents of ww_sksocialmap as JSON.

        :return: Contents of ww_sksocialmap as JSON
        """
        cursor = self.database_connection.cursor(dictionary=True)
        query = """
            SELECT sksocialmapid, scorekeeperid, networkid, account, url
            FROM ww_sksocialmap
            ORDER BY sksocialmapid ASC;
            """
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        if results:
            return json.dumps(results, indent=4, sort_keys=False)
