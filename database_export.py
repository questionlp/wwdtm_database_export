# Copyright (c) 2022-2026 Linh Pham
# wwdtm_database_export is released under the terms of the Apache License 2.0
# SPDX-License-Identifier: Apache-2.0
#
# vim: set noai syntax=python ts=4 sw=4:
"""Wait Wait Stats Database Export."""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from export.external_sites import ExternalSites
from export.guests import Guests
from export.hosts import Hosts
from export.locations import Locations
from export.metadata import Metadata
from export.panelists import Panelists
from export.postal_abbreviations import PostalAbbreviations
from export.pronouns import Pronouns
from export.scorekeepers import Scorekeepers
from export.show_descriptions import ShowDescriptions
from export.show_notes import ShowNotes
from export.shows import Shows
from export.social_media_networks import SocialMediaNetworks


def load_database_config() -> dict[str, Any]:
    """Load database connection settings from config.json.

    :return: A dictionary containing database connection settings for
        use with mysql.connector.
    """
    config_file_path = Path("config.json")
    with config_file_path.open(mode="r", encoding="utf-8") as config_file:
        config_dict = json.load(config_file)
        if "database" in config_dict:
            return config_dict["database"]


def load_settings() -> dict[str, Any]:
    """Load application settings from config.json.

    :return: A dictionary containing application settings.
    """
    config_file_path = Path("config.json")
    with config_file_path.open(mode="r", encoding="utf-8") as config_file:
        config_dict: dict = json.load(config_file)
        if "settings" in config_dict:
            settings = config_dict["settings"]
            settings["convert_decimal_to_string"] = bool(
                settings.get("convert_decimal_to_string", False)
            )
            return settings


def parse_arguments():
    """Parse arguments that are passed in when the program was run."""
    parser = argparse.ArgumentParser(
        description=("Export Wait Wait Stats database tables in JSON format.")
    )
    parser.add_argument(
        "output",
        metavar="output",
        type=str,
        nargs="?",
        default="output",
        help="Output directory",
    )
    parser.add_argument(
        "--date",
        action="store_true",
        help="Create a subdirectory under the output directory with date/timestamp",
    )
    args = parser.parse_args()
    return {
        "date": args.date,
        "output": args.output,
    }


def parse_output_directory(arguments: dict[str, Any]) -> Path:
    """Parse output directory based on command arguments.

    :param arguments: Parsed command line arguments
    :return: Path object containing the output directory path.
    """
    if "output" in arguments:
        path = Path(arguments["output"])

    if "date" in arguments and arguments["date"]:
        path = path.joinpath(datetime.now().strftime("%Y%m%d-%H%M%S%f"))

    return path


def create_output_directory(output_directory: Path):
    """Attempt to create the output directory, if it doesn't exist.

    :param output_directory: Path object containing the output directory
        path.
    """
    if not output_directory.exists():
        try:
            output_directory.mkdir(parents=True)
        except PermissionError:
            print(f"Could not create directory {output_directory}. Exiting.")
            sys.exit(1)


def export_external_sites_table(output_directory: Path):
    """Export ww_external_sites table to a JSON file.

    :param output_directory: Path object containing the output directory
        path.
    """
    external_sites = ExternalSites(connect_dict=database_config)
    external_sites_json = external_sites.to_json()
    external_sites_file_name = "ww_external_sites.json"
    external_sites_path = output_directory.joinpath(external_sites_file_name)
    try:
        with external_sites_path.open("w", encoding="utf-8") as external_sites_file:
            external_sites_file.write(external_sites_json)
    except PermissionError:
        print(f"Could not create or write to {external_sites_path}. Exiting.")
        sys.exit(1)


def export_guests_table(output_directory: Path):
    """Export ww_guests table to a JSON file.

    :param output_directory: Path object containing the output directory
        path.
    """
    guests = Guests(connect_dict=database_config)
    guests_json = guests.to_json()
    guests_file_name = "ww_guests.json"
    guests_path = output_directory.joinpath(guests_file_name)
    try:
        with guests_path.open("w", encoding="utf-8") as guests_file:
            guests_file.write(guests_json)
    except PermissionError:
        print(f"Could not create or write to {guests_path}. Exiting.")
        sys.exit(1)


def export_hosts_table(output_directory: Path):
    """Export host-related tables to a JSON file.

    :param output_directory: Path object containing the output directory
        path.
    """
    hosts = Hosts(connect_dict=database_config)
    hosts_json = hosts.to_json()
    hosts_file_name = "ww_hosts.json"
    hosts_path = output_directory.joinpath(hosts_file_name)
    try:
        with hosts_path.open("w", encoding="utf-8") as hosts_file:
            hosts_file.write(hosts_json)
    except PermissionError:
        print(f"Could not create or write to {hosts_path}. Exiting.")
        sys.exit(1)

    host_external_sites_json = hosts.external_sites_map_to_json()
    host_external_sites_file_name = "ww_hostextsitesmap.json"
    host_external_sites_path = output_directory.joinpath(host_external_sites_file_name)
    try:
        with host_external_sites_path.open(
            "w", encoding="utf-8"
        ) as host_external_sites_file:
            host_external_sites_file.write(host_external_sites_json)
    except PermissionError:
        print(f"Could not create or write to {host_external_sites_path}. Exiting.")
        sys.exit(1)

    host_pronouns_json = hosts.pronouns_map_to_json()
    host_pronouns_file_name = "ww_hostpronounsmap.json"
    host_pronouns_path = output_directory.joinpath(host_pronouns_file_name)
    try:
        with host_pronouns_path.open("w", encoding="utf-8") as host_pronouns_file:
            host_pronouns_file.write(host_pronouns_json)
    except PermissionError:
        print(f"Could not create or write to {host_pronouns_path}. Exiting.")
        sys.exit(1)

    host_social_media_networks_json = hosts.social_media_accounts_map_to_json()
    host_social_media_networks_file_name = "ww_hostsocialmap.json"
    host_social_media_networks_path = output_directory.joinpath(
        host_social_media_networks_file_name
    )
    try:
        with host_social_media_networks_path.open(
            "w", encoding="utf-8"
        ) as host_social_media_networks_file:
            host_social_media_networks_file.write(host_social_media_networks_json)
    except PermissionError:
        print(
            f"Could not create or write to {host_social_media_networks_path}. Exiting."
        )
        sys.exit(1)


def export_locations_table(
    output_directory: Path, convert_decimal_to_string: bool = True
):
    """Export ww_locations table to a JSON file.

    :param output_directory: Path object containing the output directory
        path.
    :param convert_decimal_to_string: Convert decimal values to a string
        instead of a floating point value.
    """
    locations = Locations(connect_dict=database_config)
    locations_json = locations.to_json(
        convert_decimal_to_string=convert_decimal_to_string
    )
    locations_file_name = "ww_locations.json"
    locations_path = output_directory.joinpath(locations_file_name)
    try:
        with locations_path.open("w", encoding="utf-8") as locations_file:
            locations_file.write(locations_json)
    except PermissionError:
        print(f"Could not create or write to {locations_path}. Exiting.")
        sys.exit(1)


def export_metadata_table(output_directory: Path):
    """Export __metadata table to a JSON file.

    :param output_directory: Path object containing the output directory
        path.
    """
    metadata = Metadata(connect_dict=database_config)
    metadata_json = metadata.to_json()
    metadata_file_name = "__metadata.json"
    metadata_path = output_directory.joinpath(metadata_file_name)
    try:
        with metadata_path.open("w", encoding="utf-8") as metadata_file:
            metadata_file.write(metadata_json)
    except PermissionError:
        print(f"Could not create or write to {metadata_path}. Exiting.")
        sys.exit(1)


def export_panelists_table(output_directory: Path):
    """Export panelist-related tables to a JSON file.

    :param output_directory: Path object containing the output directory
        path.
    """
    panelists = Panelists(connect_dict=database_config)
    panelists_json = panelists.to_json()
    panelists_file_name = "ww_panelists.json"
    panelists_path = output_directory.joinpath(panelists_file_name)
    try:
        with panelists_path.open("w", encoding="utf-8") as panelists_file:
            panelists_file.write(panelists_json)
    except PermissionError:
        print(f"Could not create or write to {panelists_path}. Exiting.")
        sys.exit(1)

    panelist_external_sites_json = panelists.external_sites_map_to_json()
    panelist_external_sites_file_name = "ww_panelistextsitesmap.json"
    panelist_external_sites_path = output_directory.joinpath(
        panelist_external_sites_file_name
    )
    try:
        with panelist_external_sites_path.open(
            "w", encoding="utf-8"
        ) as panelist_external_sites_file:
            panelist_external_sites_file.write(panelist_external_sites_json)
    except PermissionError:
        print(f"Could not create or write to {panelist_external_sites_path}. Exiting.")
        sys.exit(1)

    panelist_pronouns_json = panelists.pronouns_map_to_json()
    panelist_pronouns_file_name = "ww_panelistpronounsmap.json"
    panelist_pronouns_path = output_directory.joinpath(panelist_pronouns_file_name)
    try:
        with panelist_pronouns_path.open(
            "w", encoding="utf-8"
        ) as panelist_pronouns_file:
            panelist_pronouns_file.write(panelist_pronouns_json)
    except PermissionError:
        print(f"Could not create or write to {panelist_pronouns_path}. Exiting.")
        sys.exit(1)

    panelist_social_media_networks_json = panelists.social_media_accounts_map_to_json()
    panelist_social_media_networks_file_name = "ww_panelistsocialmap.json"
    panelist_social_media_networks_path = output_directory.joinpath(
        panelist_social_media_networks_file_name
    )
    try:
        with panelist_social_media_networks_path.open(
            "w", encoding="utf-8"
        ) as panelist_social_media_networks_file:
            panelist_social_media_networks_file.write(
                panelist_social_media_networks_json
            )
    except PermissionError:
        print(
            f"Could not create or write to {panelist_social_media_networks_path}. Exiting."
        )
        sys.exit(1)


def export_postal_abbreviations_table(output_directory: Path):
    """Export ww_postal_abbreviations table to a JSON file.

    :param output_directory: Path object containing the output directory
        path.
    """
    postal_abbreviations = PostalAbbreviations(connect_dict=database_config)
    postal_abbreviations_json = postal_abbreviations.to_json()
    postal_abbreviations_file_name = "ww_pronouns.json"
    postal_abbreviations_path = output_directory.joinpath(
        postal_abbreviations_file_name
    )
    try:
        with postal_abbreviations_path.open(
            "w", encoding="utf-8"
        ) as postal_abbreviations_file:
            postal_abbreviations_file.write(postal_abbreviations_json)
    except PermissionError:
        print(f"Could not create or write to {postal_abbreviations_path}. Exiting.")
        sys.exit(1)


def export_pronouns_table(output_directory: Path):
    """Export ww_pronouns table to a JSON file.

    :param output_directory: Path object containing the output directory
        path.
    """
    pronouns = Pronouns(connect_dict=database_config)
    pronouns_json = pronouns.to_json()
    pronouns_file_name = "ww_pronouns.json"
    pronouns_path = output_directory.joinpath(pronouns_file_name)
    try:
        with pronouns_path.open("w", encoding="utf-8") as panelists_file:
            panelists_file.write(pronouns_json)
    except PermissionError:
        print(f"Could not create or write to {pronouns_path}. Exiting.")
        sys.exit(1)


def export_scorekeepers_table(output_directory: Path):
    """Export scorekeeper-related tables to a JSON file.

    :param output_directory: Path object containing the output directory
        path.
    """
    scorekeepers = Scorekeepers(connect_dict=database_config)
    scorekeepers_json = scorekeepers.to_json()
    scorekeepers_file_name = "ww_scorekeepers.json"
    scorekeepers_path = output_directory.joinpath(scorekeepers_file_name)
    try:
        with scorekeepers_path.open("w", encoding="utf-8") as scorekeepers_file:
            scorekeepers_file.write(scorekeepers_json)
    except PermissionError:
        print(f"Could not create or write to {scorekeepers_path}. Exiting.")
        sys.exit(1)

    scorekeeper_external_sites_json = scorekeepers.external_sites_map_to_json()
    scorekeeper_external_sites_file_name = "ww_skextsitesmap.json"
    scorekeeper_external_sites_path = output_directory.joinpath(
        scorekeeper_external_sites_file_name
    )
    try:
        with scorekeeper_external_sites_path.open(
            "w", encoding="utf-8"
        ) as scorekeeper_external_sites_file:
            scorekeeper_external_sites_file.write(scorekeeper_external_sites_json)
    except PermissionError:
        print(
            f"Could not create or write to {scorekeeper_external_sites_path}. Exiting."
        )
        sys.exit(1)

    scorekeeper_pronouns_json = scorekeepers.pronouns_map_to_json()
    scorekeeper_pronouns_file_name = "ww_skpronounsmap.json"
    scorekeeper_pronouns_path = output_directory.joinpath(
        scorekeeper_pronouns_file_name
    )
    try:
        with scorekeeper_pronouns_path.open(
            "w", encoding="utf-8"
        ) as scorekeeper_pronouns_file:
            scorekeeper_pronouns_file.write(scorekeeper_pronouns_json)
    except PermissionError:
        print(f"Could not create or write to {scorekeeper_pronouns_path}. Exiting.")
        sys.exit(1)

    scorekeeper_social_media_networks_json = (
        scorekeepers.social_media_accounts_map_to_json()
    )
    scorekeeper_social_media_networks_file_name = "ww_sksocialmap.json"
    scorekeeper_social_media_networks_path = output_directory.joinpath(
        scorekeeper_social_media_networks_file_name
    )
    try:
        with scorekeeper_social_media_networks_path.open(
            "w", encoding="utf-8"
        ) as scorekeeper_social_media_networks_file:
            scorekeeper_social_media_networks_file.write(
                scorekeeper_social_media_networks_json
            )
    except PermissionError:
        print(
            f"Could not create or write to {scorekeeper_social_media_networks_path}. Exiting."
        )
        sys.exit(1)


def export_show_descriptions_table(output_directory: Path):
    """Export ww_showdescriptions table to a JSON file.

    :param output_directory: Path object containing the output directory
        path.
    """
    descriptions = ShowDescriptions(connect_dict=database_config)
    descriptions_json = descriptions.to_json()
    descriptions_file_name = "ww_showdescriptions.json"
    descriptions_path = output_directory.joinpath(descriptions_file_name)
    try:
        with descriptions_path.open("w", encoding="utf-8") as description_file:
            description_file.write(descriptions_json)
    except PermissionError:
        print(f"Could not create or write to {descriptions_path}. Exiting.")
        sys.exit(1)


def export_show_notes_table(output_directory: Path):
    """Export ww_shownotes table to a JSON file.

    :param output_directory: Path object containing the output directory
        path.
    """
    notes = ShowNotes(connect_dict=database_config)
    notes_json = notes.to_json()
    notes_file_name = "ww_shownotes.json"
    notes_path = output_directory.joinpath(notes_file_name)
    try:
        with notes_path.open("w", encoding="utf-8") as notes_file:
            notes_file.write(notes_json)
    except PermissionError:
        print(f"Could not create or write to {notes_path}. Exiting.")
        sys.exit(1)


def export_shows_tables(output_directory: Path, convert_decimal_to_string: bool = True):
    """Export show-related tables to corresponding JSON files.

    :param output_directory: Path object containing the output directory
        path.
    :param convert_decimal_to_string: Convert decimal values to a string
        instead of a floating point value.
    """
    shows = Shows(connect_dict=database_config)
    shows_json = shows.to_json()
    shows_file_name = "ww_shows.json"
    shows_path = output_directory.joinpath(shows_file_name)
    try:
        with shows_path.open("w", encoding="utf-8") as shows_file:
            shows_file.write(shows_json)
    except PermissionError:
        print(f"Could not create or write to {shows_path}. Exiting.")
        sys.exit(1)

    guest_map_json = shows.guest_map_to_json()
    guest_map_file_name = "ww_showguestmap.json"
    guest_map_path = output_directory.joinpath(guest_map_file_name)
    try:
        with guest_map_path.open("w", encoding="utf-8") as guest_map_file:
            guest_map_file.write(guest_map_json)
    except PermissionError:
        print(f"Could not create or write to {guest_map_path}. Exiting.")
        sys.exit(1)

    host_map_json = shows.host_map_to_json()
    host_map_file_name = "ww_showhostmap.json"
    host_map_path = output_directory.joinpath(host_map_file_name)
    try:
        with host_map_path.open("w", encoding="utf-8") as host_map_file:
            host_map_file.write(host_map_json)
    except PermissionError:
        print(f"Could not create or write to {host_map_path}. Exiting.")
        sys.exit(1)

    host_map_json = shows.host_map_to_json()
    host_map_file_name = "ww_showhostmap.json"
    host_map_path = output_directory.joinpath(host_map_file_name)
    try:
        with host_map_path.open("w", encoding="utf-8") as host_map_file:
            host_map_file.write(host_map_json)
    except PermissionError:
        print(f"Could not create or write to {host_map_path}. Exiting.")
        sys.exit(1)

    location_map_json = shows.location_map_to_json()
    location_map_file_name = "ww_showlocationmap.json"
    location_map_path = output_directory.joinpath(location_map_file_name)
    try:
        with location_map_path.open("w", encoding="utf-8") as location_map_file:
            location_map_file.write(location_map_json)
    except PermissionError:
        print(f"Could not create or write to {location_map_path}. Exiting.")
        sys.exit(1)

    panelist_map_json = shows.panelist_map_to_json(
        convert_decimal_to_string=convert_decimal_to_string
    )
    panelist_map_file_name = "ww_showpnlmap.json"
    panelist_map_path = output_directory.joinpath(panelist_map_file_name)
    try:
        with panelist_map_path.open("w", encoding="utf-8") as panelist_map_file:
            panelist_map_file.write(panelist_map_json)
    except PermissionError:
        print(f"Could not create or write to {panelist_map_path}. Exiting.")
        sys.exit(1)

    scorekeeper_map_json = shows.scorekeeper_map_to_json()
    scorekeeper_map_file_name = "ww_showskmap.json"
    scorekeeper_map_path = output_directory.joinpath(scorekeeper_map_file_name)
    try:
        with scorekeeper_map_path.open("w", encoding="utf-8") as scorekeeper_map_file:
            scorekeeper_map_file.write(scorekeeper_map_json)
    except PermissionError:
        print(f"Could not create or write to {scorekeeper_map_path}. Exiting.")
        sys.exit(1)


def export_social_media_networks_table(output_directory: Path):
    """Export ww_socialmedianetworks table to a JSON file.

    :param output_directory: Path object containing the output directory
        path.
    """
    social_media_networks = SocialMediaNetworks(connect_dict=database_config)
    social_media_networks_json = social_media_networks.to_json()
    social_media_networks_file_name = "ww_socialmedianetworks.json"
    social_media_networks_path = output_directory.joinpath(
        social_media_networks_file_name
    )
    try:
        with social_media_networks_path.open(
            "w", encoding="utf-8"
        ) as social_media_networks_file:
            social_media_networks_file.write(social_media_networks_json)
    except PermissionError:
        print(f"Could not create or write to {social_media_networks_path}. Exiting.")
        sys.exit(1)


# Load in config.json
database_config = load_database_config()
settings_config = load_settings()

# Parse command arguments and set the output directory
parsed_arguments = parse_arguments()
output_directory = parse_output_directory(parsed_arguments)

# Create the output directory
create_output_directory(output_directory=output_directory)

# Export database tables to JSON
export_external_sites_table(output_directory=output_directory)
export_guests_table(output_directory=output_directory)
export_hosts_table(output_directory=output_directory)
export_locations_table(
    output_directory=output_directory,
    convert_decimal_to_string=settings_config["convert_decimal_to_string"],
)
export_metadata_table(output_directory=output_directory)
export_panelists_table(output_directory=output_directory)
export_postal_abbreviations_table(output_directory=output_directory)
export_pronouns_table(output_directory=output_directory)
export_scorekeepers_table(output_directory=output_directory)
export_show_descriptions_table(output_directory=output_directory)
export_show_notes_table(output_directory=output_directory)
export_shows_tables(
    output_directory=output_directory,
    convert_decimal_to_string=settings_config["convert_decimal_to_string"],
)
export_social_media_networks_table(output_directory=output_directory)
