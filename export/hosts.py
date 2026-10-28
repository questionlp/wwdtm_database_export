# Copyright (c) 2022-2026 Linh Pham
# wwdtm_database_export is released under the terms of the Apache License 2.0
# SPDX-License-Identifier: Apache-2.0
#
# vim: set noai syntax=python ts=4 sw=4:
"""Hosts Tables Export Module."""

import json
from typing import Any

from mysql.connector import connect
from mysql.connector.connection import MySQLConnection
from mysql.connector.pooling import PooledMySQLConnection


class Hosts:
    """Wait Wait Stats Database Hosts.

    This class contains database methods used to export host data
    from a copy of the Wait Wait Stats database.

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
        """Returns database contents of ww_hosts as JSON.

        :return: Contents of the ww_hosts table as JSON
        """
        cursor = self.database_connection.cursor(dictionary=True)
        query = """
            SELECT hostid, host, hostgender, hostslug
            FROM ww_hosts
            ORDER BY hostid ASC;
            """
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        if results:
            return json.dumps(results, indent=4, sort_keys=False)

    def external_sites_map_to_json(self) -> str:
        """Returns database contents of ww_hostextsitesmap as JSON.

        :return: Contents of ww_hostextsitesmap as JSON
        """
        cursor = self.database_connection.cursor(dictionary=True)
        query = """
            SELECT hostextsitesmapid, hostid, siteid, url, account
            FROM ww_hostextsitesmap
            ORDER BY hostextsitesmapid ASC;
            """
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        if results:
            return json.dumps(results, indent=4, sort_keys=False)

    def pronouns_map_to_json(self) -> str:
        """Returns database contents of ww_hostpronounsmap as JSON.

        :return: Contents of ww_hostpronounsmap as JSON
        """
        cursor = self.database_connection.cursor(dictionary=True)
        query = """
            SELECT hostpronounsmapid, hostid, pronounsid
            FROM ww_hostpronounsmap
            ORDER BY hostpronounsmapid ASC;
            """
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        if results:
            return json.dumps(results, indent=4, sort_keys=False)

    def social_media_accounts_map_to_json(self) -> str:
        """Returns database contents of ww_hostsocialmap as JSON.

        :return: Contents of ww_hostsocialmap as JSON
        """
        cursor = self.database_connection.cursor(dictionary=True)
        query = """
            SELECT hostsocialmapid, hostid, networkid, account, url
            FROM ww_hostsocialmap
            ORDER BY hostsocialmapid ASC;
            """
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        if results:
            return json.dumps(results, indent=4, sort_keys=False)
