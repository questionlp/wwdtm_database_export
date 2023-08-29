# -*- coding: utf-8 -*-
# vim: set noai syntax=python ts=4 sw=4:
#
# Copyright (c) 2022 Linh Pham
# wwdtm_database_export is released under the terms of the Apache License 2.0
"""Shows and Show Mappings Database Export Module"""

import json
from typing import Any, Dict, Optional

from mysql.connector import connect


class Shows:
    """This class contains database methods used to export show and
    show mapping data from a copy of the Wait Wait Stats database.

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
        """Returns the contents of the ww_shows database table as a
        JSON string.

        :return: Contents of the ww_shows table as JSON
        """
        cursor = self.database_connection.cursor(named_tuple=True)
        query = (
            "SELECT showid, showdate, repeatshowid, bestof, "
            "bestofuniquebluff "
            "FROM ww_shows "
            "ORDER BY showid ASC;"
        )
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
        """Returns the contents of the ww_showbluffmap database table as
        a JSON string.

        :return: Contents of the ww_showbluffmap table as JSON
        """
        cursor = self.database_connection.cursor(dictionary=True)
        query = (
            "SELECT showbluffmapid, showid, chosenbluffpnlid, "
            "correctbluffpnlid "
            "FROM ww_showbluffmap "
            "ORDER BY showbluffmapid ASC;"
        )
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        if results:
            return json.dumps(results, indent=2, sort_keys=False)

    def guest_map_to_json(self) -> str:
        """Returns the contents of the ww_showguestmap database table as
        a JSON string.

        :return: Contents of the ww_showguestmap table as JSON
        """
        cursor = self.database_connection.cursor(dictionary=True)
        query = (
            "SELECT showguestmapid, showid, guestid, guestscore, exception "
            "FROM ww_showguestmap "
            "ORDER BY showguestmapid ASC;"
        )
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        if results:
            return json.dumps(results, indent=2, sort_keys=False)

    def host_map_to_json(self) -> str:
        """Returns the contents of the ww_showhostmap database table as
        a JSON string.

        :return: Contents of the ww_showhostmap table as JSON
        """
        cursor = self.database_connection.cursor(dictionary=True)
        query = (
            "SELECT showhostmapid, showid, hostid, guest "
            "FROM ww_showhostmap "
            "ORDER BY showhostmapid ASC;"
        )
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        if results:
            return json.dumps(results, indent=2, sort_keys=False)

    def location_map_to_json(self) -> str:
        """Returns the contents of the ww_showlocationmap database table
        as a JSON string.

        :return: Contents of the ww_showlocationmap table as JSON
        """
        cursor = self.database_connection.cursor(dictionary=True)
        query = (
            "SELECT showlocationmapid, showid, locationid "
            "FROM ww_showlocationmap "
            "ORDER BY showlocationmapid ASC;"
        )
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        if results:
            return json.dumps(results, indent=2, sort_keys=False)

    def panelist_map_to_json(self, include_decimal_score: bool = False) -> str:
        """Returns the contents of the ww_showpnlmap database table as a
        JSON string.

        :return: Contents of the ww_showpnlmap table as JSON
        """
        query = "SHOW COLUMNS FROM ww_showpnlmap WHERE Field = 'panelistscore_decimal';"
        cursor = self.database_connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()

        if result:
            has_panelist_score_decimal = True
        else:
            has_panelist_score_decimal = False

        if include_decimal_score and has_panelist_score_decimal:
            query = (
                "SELECT showpnlmapid, showid, panelistid, panelistlrndstart, "
                "panelistlrndcorrect, panelistscore, panelistscore_decimal, "
                "showpnlrank "
                "FROM ww_showpnlmap "
                "ORDER BY showpnlmapid ASC;"
            )
        else:
            query = (
                "SELECT showpnlmapid, showid, panelistid, panelistlrndstart, "
                "panelistlrndcorrect, panelistscore, showpnlrank "
                "FROM ww_showpnlmap "
                "ORDER BY showpnlmapid ASC;"
            )
        cursor = self.database_connection.cursor(dictionary=True)
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        if results:
            if include_decimal_score:
                records = []
                for row in results:
                    if "panelistscore_decimal" in row and row["panelistscore_decimal"]:
                        row["panelistscore_decimal"] = float(
                            row["panelistscore_decimal"]
                        )
                        records.append(row)

                return json.dumps(records, indent=2, sort_keys=False)
            else:
                return json.dumps(results, indent=2, sort_keys=False)

    def scorekeeper_map_to_json(self) -> str:
        """Returns the contents of the ww_showskmap database table as a
        JSON string.

        :return: Contents of the ww_showskmap table as JSON
        """
        cursor = self.database_connection.cursor(dictionary=True)
        query = (
            "SELECT showskmapid, showid, scorekeeperid, guest, description "
            "FROM ww_showskmap "
            "ORDER BY showskmapid ASC;"
        )
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()

        if results:
            return json.dumps(results, indent=2, sort_keys=False)
