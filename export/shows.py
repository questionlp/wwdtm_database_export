# Copyright (c) 2022-2024 Linh Pham
# wwdtm_database_export is released under the terms of the Apache License 2.0
# SPDX-License-Identifier: Apache-2.0
#
# vim: set noai syntax=python ts=4 sw=4:
"""Shows and Show Mappings Database Export Module."""

import json
from typing import Any

from mysql.connector import connect
from mysql.connector.connection import MySQLConnection
from mysql.connector.pooling import PooledMySQLConnection


class Shows:
    """Wait Wait Stats Database Shows and Show Mappins.

    This class contains database methods used to export show and show
    mapping data from a copy of the Wait Wait Stats database.

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
        """Returns database contents of ww_shows as JSON.

        :return: Contents of the ww_shows table as JSON
        """
        cursor = self.database_connection.cursor(named_tuple=True)
        query = """
            SELECT showid, showdate, repeatshowid, bestof, bestofuniquebluff
            FROM ww_shows
            ORDER BY showid ASC;
            """
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        if results:
            shows = []
            for show in results:
                info = {
                    "showid": show.showid,
                    "showdate": show.showdate.isoformat(),
                    "repeatshowid": show.repeatshowid,
                    "bestof": show.bestof,
                    "bestofuniquebluff": show.bestofuniquebluff,
                }
                shows.append(info)

            return json.dumps(shows, indent=2, sort_keys=False)

    def bluff_map_to_json(self) -> str:
        """Returns database contents of ww_showbluffmap as JSON.

        :return: Contents of the ww_showbluffmap table as JSON
        """
        cursor = self.database_connection.cursor(dictionary=True)
        query = """
            SELECT showbluffmapid, showid, chosenbluffpnlid, correctbluffpnlid
            FROM ww_showbluffmap
            ORDER BY showbluffmapid ASC;
            """
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        if results:
            return json.dumps(results, indent=2, sort_keys=False)

    def guest_map_to_json(self) -> str:
        """Returns database contents of ww_showguestmap as JSON.

        :return: Contents of the ww_showguestmap table as JSON
        """
        cursor = self.database_connection.cursor(dictionary=True)
        query = """
            SELECT showguestmapid, showid, guestid, guestscore, exception
            FROM ww_showguestmap
            ORDER BY showguestmapid ASC;
            """
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        if results:
            return json.dumps(results, indent=2, sort_keys=False)

    def host_map_to_json(self) -> str:
        """Returns database contents of ww_showhostmap as JSON.

        :return: Contents of the ww_showhostmap table as JSON
        """
        cursor = self.database_connection.cursor(dictionary=True)
        query = """
            SELECT showhostmapid, showid, hostid, guest
            FROM ww_showhostmap
            ORDER BY showhostmapid ASC;
            """
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        if results:
            return json.dumps(results, indent=2, sort_keys=False)

    def location_map_to_json(self) -> str:
        """Returns database contents of ww_showlocationmap as JSON.

        :return: Contents of the ww_showlocationmap table as JSON
        """
        cursor = self.database_connection.cursor(dictionary=True)
        query = """
            SELECT showlocationmapid, showid, locationid
            FROM ww_showlocationmap
            ORDER BY showlocationmapid ASC;
            """
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        if results:
            return json.dumps(results, indent=2, sort_keys=False)

    def panelist_map_to_json(self, convert_decimal_to_string: bool = False) -> str:
        """Returns database contents of ww_showpnlmap database as JSON.

        :return: Contents of the ww_showpnlmap table as JSON
        """
        query = """
            SELECT showpnlmapid, showid, panelistid,
            panelistlrndstart, panelistlrndstart_decimal,
            panelistlrndcorrect, panelistlrndcorrect_decimal,
            panelistscore, panelistscore_decimal, showpnlrank
            FROM ww_showpnlmap
            ORDER BY showpnlmapid ASC;
            """

        cursor = self.database_connection.cursor(named_tuple=True)
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        if results:
            shows = []
            for row in results:
                if row.panelistlrndstart_decimal:
                    panelistlrndstart_decimal = (
                        str(row.panelistlrndstart_decimal)
                        if convert_decimal_to_string
                        else float(row.panelistlrndstart_decimal)
                    )
                else:
                    panelistlrndstart_decimal = None

                if row.panelistlrndcorrect_decimal:
                    panelistlrndcorrect_decimal = (
                        str(row.panelistlrndcorrect_decimal)
                        if convert_decimal_to_string
                        else float(row.panelistlrndcorrect_decimal)
                    )
                else:
                    panelistlrndcorrect_decimal = None

                if row.panelistscore_decimal:
                    panelistscore_decimal: str | float = (
                        str(row.panelistscore_decimal)
                        if convert_decimal_to_string
                        else float(row.panelistscore_decimal)
                    )
                else:
                    panelistscore_decimal = None

                shows.append(
                    {
                        "showpnlmapid": row.showpnlmapid,
                        "showid": row.showid,
                        "panelistid": row.panelistid,
                        "panelistlrndstart": row.panelistlrndstart,
                        "panelistlrndstart_decimal": panelistlrndstart_decimal,
                        "panelistlrndcorrect": row.panelistlrndcorrect,
                        "panelistlrndcorrect_decimal": panelistlrndcorrect_decimal,
                        "panelistscore": row.panelistscore,
                        "panelistscore_decimal": panelistscore_decimal,
                        "showpnlrank": row.showpnlrank,
                    }
                )

            return json.dumps(shows, indent=2, sort_keys=False)

    def scorekeeper_map_to_json(self) -> str:
        """Returns database contents of ww_showskmap as JSON.

        :return: Contents of the ww_showskmap table as JSON
        """
        cursor = self.database_connection.cursor(dictionary=True)
        query = """
            SELECT showskmapid, showid, scorekeeperid, guest, description
            FROM ww_showskmap
            ORDER BY showskmapid ASC;
            """
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        if results:
            return json.dumps(results, indent=2, sort_keys=False)
