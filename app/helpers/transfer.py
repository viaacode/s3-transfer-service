#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import shlex
import threading
import time
from typing import List

import requests
from paramiko import AutoAddPolicy, SSHClient, SSHException
from retry import retry
from viaa.configuration import ConfigParser
from viaa.observability import logging


config_parser = ConfigParser()
config = config_parser.app_cfg
log = logging.get_logger(__name__, config=config_parser)
dest_conf = config["destination"]
NUMBER_PARTS = 4


class TransferException(Exception):
    pass


def build_curl_command(destination: str, source_url: str, s3_domain: str) -> str:
    """Build the cURL command.

    The args "-S -s" are used so that the progress bar is not shown but errors are.
    In combination with "-w", it will output information of the download after
    completion.

    Args:
        destination: Full filename path of destination file.
        source_url: The URL to fetch the file from.
        s3_domain: The S3 domain to pass as header.

    Returns:
        The cURL command shell-escaped
    """
    command = [
        "curl",
        "-w",
        "%{http_code},time: %{time_total}s,size: %{size_download} bytes,speed: %{speed_download}b/s",
        "-L",
        "-H",
        f"host: {s3_domain}",
        "-S",
        "-s",
        "-o",
        destination,
        source_url,
    ]
    return shlex.join(command)


class Transfer:
    def __init__(self, message: dict):
        """Initialize a Transfer.

        Args:
            message: Contains the information of the source file and the destination
                filename."""
        self.domain = message["source"]["domain"]["name"]
        self.destination_path = message["destination"]["path"]

        self.dest_folder_dirname = os.path.dirname(self.destination_path)
        self.dest_file_basename = os.path.basename(self.destination_path)

        self.dest_file_tmp_basename = f"{self.dest_file_basename}.tmp"
        dest_folder_tmp_basename = f"{self.dest_file_basename}.part"
        self.dest_folder_tmp_dirname = os.path.join(
            self.dest_folder_dirname, dest_folder_tmp_basename
        )

        # Full filename of the tmp file
        self.dest_file_tmp_full = os.path.join(
            self.dest_folder_tmp_dirname,
            self.dest_file_tmp_basename,
        )

        bucket = message["source"]["bucket"]["name"]
        key = message["source"]["object"]["key"]
        self.source_url = f"http://{config['source']['swarmurl']}/{bucket}/{key}"
        self.size_in_bytes = 0

        # SSH client
        self.remote_client = None
        # SFTP client
        self.sftp = None

    def _init_remote_client(self):
        # SSH client
        self.remote_client = SSHClient()
        self.remote_client.set_missing_host_key_policy(AutoAddPolicy())
        self.remote_client.connect(
            dest_conf["host"],
            port=22,
            username=dest_conf["user"],
            password=dest_conf["password"],
        )
        # SFTP client
        self.sftp = self.remote_client.open_sftp()

    def _fetch_size(self) -> int:
        """Fetch the size of the file on Castor.

        The size is in the "content-length" response header.

        Returns:
            The size of the file in bytes.

        Raises:
            TransferException: If it was not possible to get the size of the file,
                e.g. a 404.
        """

        size_in_bytes = requests.head(
            self.source_url,
            allow_redirects=True,
            headers={"host": self.domain, "Accept-Encoding": "identity"},
        ).headers.get("content-length", None)

        if not size_in_bytes:
            log.error(
                "Failed to get size of file on Castor", source_url=self.source_url
            )
            raise TransferException

        return size_in_bytes

    def _check_free_space(self):
        """Check if there is sufficient free space on the remote server.

        The free space needs to be a higher than a given percentage in order
        to be allowed to send the file over. If the space is lower, then it
        will retry until the space is freed.

        This free space check is optional, in the sense that if one or more of
        the needed config values are empty, it will assume that a transfer is
        always allowed.
        """
        try:
            percentage_limit = int(dest_conf["free_space_percentage"])
        except ValueError:
            percentage_limit = ""
        file_system = dest_conf["file_system"]

        # If percentage limit or file system is not filled in, skip the check.
        if percentage_limit and file_system:
            while True:
                # Check the used space in percentage
                _stdin, stdout, _stderr = self.remote_client.exec_command(
                    f"df --output=pcent {file_system} | tail -1"
                )
                out = stdout.readlines()
                # Parse the used percentage as an int.
                try:
                    percentage_used = int(
                        out[0].strip().split("%")[0]  # Output example: [' 12%\n']
                    )
                except ValueError:
                    log.warning("Could not get used percentage")
                    break

                free_percentage = 100 - percentage_used
                log.info(
                    f"Free space: {free_percentage}%. Space needed: {percentage_limit}%"
                )
                if free_percentage > percentage_limit:
                    break
                else:
                    time.sleep(120)

    def _prepare_target_transfer(self):
        """Prepare for transferring the file to the remote server.

        Do the following:
        - Check if the file does not exist yet.
        - Create the tmp folder if it does not exist yet. Note that if the tmp folder
          already exists, we'll just continue.

        Raises:
            OSError:
                -The file already exists.
                -The tmp folder couldn't be created.
            TransferException: When a SSH error occurred.
        """
        # Check if file doesn't exist yet and make the tmp dir

        try:
            # Check if the file does not exist yet
            try:
                self.sftp.stat(self.destination_path)
            except FileNotFoundError:
                # Continue
                pass
            else:
                # If the file exists stop.
                log.error("File already exists", destination=self.destination_path)
                raise OSError

            # Create tmp folder if it doesn't exist yet
            try:
                self.sftp.mkdir(self.dest_folder_tmp_dirname)
            except OSError as os_e:
                # If the folder already exists, just continue
                try:
                    self.sftp.stat(self.dest_folder_tmp_dirname)
                except FileNotFoundError:
                    log.error(
                        f"Error occurred when creating tmp folder: {os_e}",
                        tmp_folder=self.dest_folder_tmp_dirname,
                    )
                    raise os_e
        except SSHException as ssh_e:
            log.error(
                f"SSH Error occurred: {ssh_e}",
                tmp_folder=self.dest_folder_tmp_dirname,
            )
            raise TransferException

    def _transfer_file(self):
        """Transfer the file."""

        # Build the cURL command
        curl_cmd = build_curl_command(
            self.dest_file_tmp_full,
            self.source_url,
            self.domain,
        )

        try:
            # Execute the cURL command and examine results
            _stdin, stdout, stderr = self.remote_client.exec_command(curl_cmd)
            results = []
            out = stdout.readlines()
            err = stderr.readlines()
            if err:
                log.error(
                    f"Error occurred when cURLing file: {err}",
                    destination=self.dest_file_tmp_full,
                )
                raise TransferException
            if out:
                try:
                    results = out[0].split(",")
                    status_code = results[0]
                    if int(status_code) >= 400:
                        log.error(
                            f"Error occurred when cURLing file with status code: {status_code}",
                            destination=self.dest_file_tmp_full,
                        )
                        raise TransferException
                    log.info(
                        "Successfully cURLed file",
                        destination=self.dest_file_tmp_full,
                        results=results,
                    )
                except IndexError as i_e:
                    log.error(
                        f"Error occurred cURLing file: {i_e}",
                        destination=self.dest_file_tmp_full,
                    )
                    raise TransferException
        except SSHException as ssh_e:
            log.error(
                f"SSH Error occurred when cURLing file: {ssh_e}",
                destination=self.dest_file_tmp_full,
            )
            raise TransferException

        try:
            # Check if file has the correct size
            # self.sftp.chdir(self.dest_folder_tmp_dirname)
            file_attrs = self.sftp.stat(self.dest_file_tmp_full)
            if file_attrs.st_size != int(self.size_in_bytes):
                log.error(
                    f"Size of transferred tmp file: {file_attrs.st_size}, expected size: {self.size_in_bytes}",
                    source_url=self.source_url,
                    destination_basename=self.self.dest_file_tmp_full_basename,
                )
                raise TransferException
            # Rename and move file destination folder
            self.sftp.rename(
                self.dest_file_tmp_full,
                self.destination_path,
            )
            # Touch the file so MH picks it up
            # Explicitly use a `SSH touch` as `SFTP utime` doesn't work
            self.remote_client.exec_command(f"touch '{self.destination_path}'")

            # Delete the tmp folder
            self.sftp.rmdir(self.dest_folder_tmp_dirname)
            log.info("File successfully transferred", destination=self.destination_path)
        except OSError as os_e:
            log.error(
                f"Error occurred when renaming tmp file: {os_e}",
                destination=self.destination_path,
            )
            raise TransferException

    @retry(TransferException, tries=3, delay=3, logger=log)
    def transfer(self):
        """Transfer a file to a remote server.

        First we'll make the tmp dir to transfer the (tmp) file to.
        Then, fetch the size of the file to check if the transferred file has same size.
        Furthermore, rename/move the tmp file to its correct destination folder.
        Lastly, remove the tmp folder.
        """

        try:
            log.info(f"Start transferring of file: {self.source_url}")

            # initialize the SSH client
            self._init_remote_client()

            # Check freespace
            self._check_free_space()

            # Fetch size of the file to transfer
            self.size_in_bytes = self._fetch_size()

            # Check if file doesn't exist yet and make the tmp dir
            self._prepare_target_transfer()

            # Transfer the parts
            self._transfer_file()

        finally:
            self.remote_client.close()
