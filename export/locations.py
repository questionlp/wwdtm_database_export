# Copyright (c) 2022-2024 Linh Pham
# wwdtm_database_export is released under the terms of the Apache License 2.0
# SPDX-License-Identifier: Apache-2.0
#
# vim: set noai syntax=python ts=4 sw=4:
"""Locations Database Export Module."""
import json
from typing import Any

from mysql.connector import connect
from mysql.connector.connection import MySQLConnection
from mysql.connector.pooling import PooledMySQLConnection


class Locations:
    """Wait Wait Stats Database Locations.

    This class contains database methods used to export location data
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

    def to_json(self, convert_decimal_to_string: bool = False) -> str:
        """Returns database contents of ww_locations as JSON.

        :return: Contents of the ww_locations table as JSON
        """
        cursor = self.database_connection.cursor(named_tuple=True)
        query = """
            SELECT locationid, city, state, venue, latitude, longitude,
            locationslug
            FROM ww_locations
            ORDER BY locationid ASC;
            """
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        locations = []
        if results:
            for row in results:
                if row.latitude:
                    latitude = (
                        str(row.latitude)
                        if convert_decimal_to_string
                        else float(row.latitude)
                    )
                else:
                    latitude = None

                if row.longitude:
                    longitude = (
                        str(row.longitude)
                        if convert_decimal_to_string
                        else float(row.longitude)
                    )
                else:
                    longitude = None

                locations.append(
                    {
                        "locationid": row.locationid,
                        "city": row.city,
                        "state": row.venue,
                        "latitude": latitude,
                        "longitude": longitude,
                        "locationslug": row.locationslug,
                    }
                )

        return json.dumps(locations, indent=2, sort_keys=False)
