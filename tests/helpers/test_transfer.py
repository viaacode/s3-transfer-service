#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pytest
from unittest.mock import MagicMock, patch

from paramiko import SSHException

from app.helpers.transfer import (
    build_curl_command,
    Transfer,
    TransferException,
)


def test_build_curl_command():
    dest = "dest file"
    src = "source file"
    domain = "S3 domain"
    w_params = "%{http_code},time: %{time_total}s,size: %{size_download} bytes,speed: %{speed_download}b/s"
    curl_command = build_curl_command(dest, src, domain)
    assert (
        curl_command
        == f"curl -w '{w_params}' -L -H 'host: {domain}' -S -s -o '{dest}' '{src}'"
    )


class TestTransfer:
    @pytest.fixture()
    @patch("app.helpers.transfer.SSHClient")
    def transfer(self, ssh_client_mock) -> Transfer:
        msg = {
            "source": {
                "domain": {"name": "domain"},
                "object": {"key": "file.mxf"},
                "bucket": {"name": "bucket"},
            },
            "destination": {"path": "/s3-transfer-test/file.mxf"},
        }
        transfer = Transfer(msg)
        transfer._init_remote_client()
        return transfer

    @patch("requests.head")
    def test_fetch_size(self, head_mock, transfer):
        """Response contains a "content-length" response header with the size."""
        # Mock return size of file
        head_mock().headers = {"content-length": 1000}

        size = transfer._fetch_size()
        assert size == 1000

    @patch("requests.head")
    def test_fetch_size_error(self, head_mock, transfer, caplog):
        """No "content-length" response header."""
        # Mock return size of file
        head_mock().headers = {}
        with pytest.raises(TransferException):
            transfer._fetch_size()
        log_record = caplog.records[0]
        assert log_record.level == "error"
        assert log_record.message == "Failed to get size of file on Castor"

    def test_prepare_target_transfer(self, transfer):
        """File does not exist and folder is created"""
        sftp_mock = transfer.sftp

        sftp_mock.stat.side_effect = FileNotFoundError

        transfer._prepare_target_transfer()

        sftp_mock.stat.assert_called_once_with("/s3-transfer-test/file.mxf")
        sftp_mock.mkdir.assert_called_once_with("/s3-transfer-test/file.mxf.part")

    def test__prepare_target_transfer_file_exists(self, transfer, caplog):
        """File already exist."""

        with pytest.raises(OSError):
            transfer._prepare_target_transfer()

        sftp_mock = transfer.sftp
        log_record = caplog.records[0]
        assert log_record.level == "error"
        assert log_record.message == "File already exists"
        assert log_record.destination == "/s3-transfer-test/file.mxf"

        sftp_mock.stat.assert_called_once_with("/s3-transfer-test/file.mxf")
        sftp_mock.mkdir.assert_not_called()

    def test_prepare_target_transfer_folder_exists(self, transfer):
        """File does not exist and tmp folder already exists."""
        sftp_mock = transfer.sftp
        # File not found but folder is found.
        sftp_mock.stat.side_effect = (FileNotFoundError, MagicMock)
        # mkdir results in OSError
        sftp_mock.mkdir.side_effect = OSError("error")

        transfer._prepare_target_transfer()

        assert sftp_mock.stat.call_count == 2
        sftp_mock.mkdir.assert_called_once_with("/s3-transfer-test/file.mxf.part")

    def test_prepare_target_transfer_folder_error(self, transfer, caplog):
        """File does not exist but tmp folder can't be created."""
        sftp_mock = transfer.sftp
        # File not found and folder not found. Gets called twice.
        sftp_mock.stat.side_effect = FileNotFoundError
        # mkdir results in OSError
        sftp_mock.mkdir.side_effect = OSError("error")

        with pytest.raises(OSError):
            transfer._prepare_target_transfer()

        log_record = caplog.records[0]
        assert log_record.level == "error"
        assert log_record.message == "Error occurred when creating tmp folder: error"
        assert log_record.tmp_folder == "/s3-transfer-test/file.mxf.part"

        assert sftp_mock.stat.call_count == 2
        sftp_mock.mkdir.assert_called_once_with("/s3-transfer-test/file.mxf.part")

    @patch("app.helpers.transfer.build_curl_command", return_value="curl")
    def test_transfer_file(self, build_curl_command_mock, transfer, caplog):
        """Successfully transfer the file."""
        stdin_mock, stdout_mock, stderr_mock = (MagicMock(), MagicMock(), MagicMock())
        # Mock the stdout result of the cURL command
        stdout_result = ["206,time: 5s,size: 1000 bytes,speed: 200b/s"]
        stdout_mock.readlines.return_value = stdout_result
        # Mock stderr to be empty
        stderr_mock.readlines.return_value = []

        client_mock = transfer.remote_client
        client_mock.exec_command.return_value = (stdin_mock, stdout_mock, stderr_mock)

        transfer.size_in_bytes = 1000
        sftp_mock = transfer.sftp
        # Mock check filesize of transferred file
        sftp_mock.stat.return_value.st_size = 1000

        transfer._transfer_file()

        # Check logged message
        log_record = caplog.records[0]
        assert log_record.level == "info"
        assert log_record.message == "Successfully cURLed tmp file"
        assert log_record.destination == "/s3-transfer-test/file.mxf.part/file.mxf.tmp"

        assert client_mock.exec_command.call_count == 2

        # Check if curl command gets called with the correct arguments
        build_curl_command_mock.assert_called_once_with(
            "/s3-transfer-test/file.mxf.part/file.mxf.tmp",
            "http://url/bucket/file.mxf",
            "domain",
        )

        # Check if tmp file is correct size
        sftp_mock.stat.assert_called_once_with(
            "/s3-transfer-test/file.mxf.part/file.mxf.tmp"
        )

        # Check if tmp file renamed
        sftp_mock.rename.assert_called_once_with(
            "/s3-transfer-test/file.mxf.part/file.mxf.tmp", "/s3-transfer-test/file.mxf"
        )

        # Check if touch command has been executed
        assert client_mock.exec_command.call_args_list[1].args == (
            "touch '/s3-transfer-test/file.mxf'",
        )

        # Check if tmp dir has been removed
        sftp_mock.rmdir.assert_called_once_with(
            "/s3-transfer-test/file.mxf.part",
        )

        # Check logged message
        log_record = caplog.records[1]
        assert log_record.level == "info"
        assert log_record.message == "File successfully transferred"
        assert log_record.destination == "/s3-transfer-test/file.mxf"

    def test_transfer_file_stderr(self, transfer, caplog):
        """Transferring a file resulting in stderr output."""
        stdin_mock, stdout_mock, stderr_mock = (MagicMock(), MagicMock(), MagicMock())
        # Mock the stderr result of the cURL command
        stderr_result = ["Error"]
        stderr_mock.readlines.return_value = stderr_result

        client_mock = transfer.remote_client
        client_mock.exec_command.return_value = (stdin_mock, stdout_mock, stderr_mock)

        with pytest.raises(TransferException):
            transfer._transfer_file()
        assert "Error occurred when cURLing tmp file: ['Error']" in caplog.messages

        sftp_mock = transfer.sftp
        # Check if tmp file not renamed
        sftp_mock.rename.assert_not_called()

        # Check if tmp dir has not been removed
        sftp_mock.rmdir.assert_not_called()

    def test_transfer_part_ssh_exception(self, transfer, caplog):
        """SSH Exception occurs when connecting."""
        client_mock = transfer.remote_client
        client_mock.exec_command.side_effect = SSHException("Connection error")
        with pytest.raises(TransferException):
            transfer._transfer_file()
        assert client_mock.exec_command.call_count == 1
        assert (
            "SSH Error occurred when cURLing tmp file: Connection error"
            in caplog.messages
        )

        sftp_mock = transfer.sftp
        # Check if tmp file not renamed
        sftp_mock.rename.assert_not_called()

        # Check if tmp dir has not been removed
        sftp_mock.rmdir.assert_not_called()

    @patch("app.helpers.transfer.build_curl_command", return_value="curl")
    def test_transfer_file_diff_file(self, build_curl_command_mock, transfer, caplog):
        """Tmp file differs from expected size."""
        stdin_mock, stdout_mock, stderr_mock = (MagicMock(), MagicMock(), MagicMock())
        # Mock the stdout result of the cURL command
        stdout_result = ["206,time: 5s,size: 1000 bytes,speed: 200b/s"]
        stdout_mock.readlines.return_value = stdout_result
        # Mock stderr to be empty
        stderr_mock.readlines.return_value = []

        client_mock = transfer.remote_client
        client_mock.exec_command.return_value = (stdin_mock, stdout_mock, stderr_mock)

        transfer.size_in_bytes = 1000
        sftp_mock = transfer.sftp
        # Mock check filesize of transferred file
        sftp_mock.stat.return_value.st_size = 500

        with pytest.raises(TransferException):
            transfer._transfer_file()

        # Check logged message
        log_record = caplog.records[0]
        assert log_record.level == "info"
        assert log_record.message == "Successfully cURLed tmp file"
        assert log_record.destination == "/s3-transfer-test/file.mxf.part/file.mxf.tmp"

        assert client_mock.exec_command.call_count == 1

        # Check if curl command gets called with the correct arguments
        build_curl_command_mock.assert_called_once_with(
            "/s3-transfer-test/file.mxf.part/file.mxf.tmp",
            "http://url/bucket/file.mxf",
            "domain",
        )

        # Check if tmp file is correct size
        sftp_mock.stat.assert_called_once_with(
            "/s3-transfer-test/file.mxf.part/file.mxf.tmp"
        )

        # Check if tmp file not renamed
        sftp_mock.rename.assert_not_called()

        # Check if tmp dir has not been removed
        sftp_mock.rmdir.assert_not_called()

        # Check logged message
        log_record = caplog.records[1]
        assert log_record.level == "error"
        assert (
            log_record.message
            == "Size of transferred tmp file: 500, expected size: 1000"
        )
        assert log_record.destination == "/s3-transfer-test/file.mxf.part/file.mxf.tmp"

    @patch("app.helpers.transfer.build_curl_command", return_value="curl")
    def test_transfer_file_stat_os_error(
        self, build_curl_command_mock, transfer, caplog
    ):
        """Error when checking filesize of tmp file."""
        stdin_mock, stdout_mock, stderr_mock = (MagicMock(), MagicMock(), MagicMock())
        # Mock the stdout result of the cURL command
        stdout_result = ["206,time: 5s,size: 1000 bytes,speed: 200b/s"]
        stdout_mock.readlines.return_value = stdout_result
        # Mock stderr to be empty
        stderr_mock.readlines.return_value = []

        client_mock = transfer.remote_client
        client_mock.exec_command.return_value = (stdin_mock, stdout_mock, stderr_mock)

        sftp_mock = transfer.sftp
        # Mock check filesize of transferred file
        sftp_mock.stat.side_effect = OSError("not found")

        with pytest.raises(TransferException):
            transfer._transfer_file()

        # Check logged message
        log_record = caplog.records[0]
        assert log_record.level == "info"
        assert log_record.message == "Successfully cURLed tmp file"
        assert log_record.destination == "/s3-transfer-test/file.mxf.part/file.mxf.tmp"

        assert client_mock.exec_command.call_count == 1

        # Check if curl command gets called with the correct arguments
        build_curl_command_mock.assert_called_once_with(
            "/s3-transfer-test/file.mxf.part/file.mxf.tmp",
            "http://url/bucket/file.mxf",
            "domain",
        )

        # Check if tmp file is correct size
        sftp_mock.stat.assert_called_once_with(
            "/s3-transfer-test/file.mxf.part/file.mxf.tmp"
        )

        # Check if tmp file not renamed
        sftp_mock.rename.assert_not_called()

        # Check if tmp dir has not been removed
        sftp_mock.rmdir.assert_not_called()

        # Check logged message
        log_record = caplog.records[1]
        assert log_record.level == "error"
        assert (
            log_record.message
            == "Error occurred when checking size of transferred tmp file: not found"
        )
        assert log_record.tmp_filename == "/s3-transfer-test/file.mxf.part/file.mxf.tmp"

    @patch("app.helpers.transfer.build_curl_command", return_value="curl")
    def test_transfer_file_rename_os_error(
        self, build_curl_command_mock, transfer, caplog
    ):
        """Error when renaming tmp file."""
        stdin_mock, stdout_mock, stderr_mock = (MagicMock(), MagicMock(), MagicMock())
        # Mock the stdout result of the cURL command
        stdout_result = ["206,time: 5s,size: 1000 bytes,speed: 200b/s"]
        stdout_mock.readlines.return_value = stdout_result
        # Mock stderr to be empty
        stderr_mock.readlines.return_value = []

        client_mock = transfer.remote_client
        client_mock.exec_command.return_value = (stdin_mock, stdout_mock, stderr_mock)

        transfer.size_in_bytes = 1000
        sftp_mock = transfer.sftp
        # Mock check filesize of transferred file
        sftp_mock.stat.return_value.st_size = 1000
        # Fail when renaming5

        sftp_mock.rename.side_effect = OSError("Insufficient rights")

        with pytest.raises(TransferException):
            transfer._transfer_file()

        # Check logged message
        log_record = caplog.records[0]
        assert log_record.level == "info"
        assert log_record.message == "Successfully cURLed tmp file"
        assert log_record.destination == "/s3-transfer-test/file.mxf.part/file.mxf.tmp"

        assert client_mock.exec_command.call_count == 1

        # Check if curl command gets called with the correct arguments
        build_curl_command_mock.assert_called_once_with(
            "/s3-transfer-test/file.mxf.part/file.mxf.tmp",
            "http://url/bucket/file.mxf",
            "domain",
        )

        # Check if tmp file is correct size
        sftp_mock.stat.assert_called_once_with(
            "/s3-transfer-test/file.mxf.part/file.mxf.tmp"
        )

        # Check if tmp file renamed
        sftp_mock.rename.assert_called_once_with(
            "/s3-transfer-test/file.mxf.part/file.mxf.tmp", "/s3-transfer-test/file.mxf"
        )

        # Check if tmp dir has not been removed
        sftp_mock.rmdir.assert_not_called()

        # Check logged message
        log_record = caplog.records[1]
        assert log_record.level == "error"
        assert (
            log_record.message
            == "Error occurred when renaming tmp file: Insufficient rights"
        )
        assert log_record.tmp_filename == "/s3-transfer-test/file.mxf.part/file.mxf.tmp"

    @patch("app.helpers.transfer.build_curl_command", return_value="curl")
    def test_transfer_file_touch_ssh_error(
        self, build_curl_command_mock, transfer, caplog
    ):
        """SSH error when touching tmp file."""
        stdin_mock, stdout_mock, stderr_mock = (MagicMock(), MagicMock(), MagicMock())
        # Mock the stdout result of the cURL command
        stdout_result = ["206,time: 5s,size: 1000 bytes,speed: 200b/s"]
        stdout_mock.readlines.return_value = stdout_result
        # Mock stderr to be empty
        stderr_mock.readlines.return_value = []

        client_mock = transfer.remote_client
        client_mock.exec_command.side_effect = [
            (stdin_mock, stdout_mock, stderr_mock),
            SSHException("No touching!"),
        ]

        transfer.size_in_bytes = 1000
        sftp_mock = transfer.sftp
        # Mock check filesize of transferred file
        sftp_mock.stat.return_value.st_size = 1000

        with pytest.raises(TransferException):
            transfer._transfer_file()

        # Check logged message
        log_record = caplog.records[0]
        assert log_record.level == "info"
        assert log_record.message == "Successfully cURLed tmp file"
        assert log_record.destination == "/s3-transfer-test/file.mxf.part/file.mxf.tmp"

        assert client_mock.exec_command.call_count == 2

        # Check if curl command gets called with the correct arguments
        build_curl_command_mock.assert_called_once_with(
            "/s3-transfer-test/file.mxf.part/file.mxf.tmp",
            "http://url/bucket/file.mxf",
            "domain",
        )

        # Check if tmp file is correct size
        sftp_mock.stat.assert_called_once_with(
            "/s3-transfer-test/file.mxf.part/file.mxf.tmp"
        )

        # Check if tmp file renamed
        sftp_mock.rename.assert_called_once_with(
            "/s3-transfer-test/file.mxf.part/file.mxf.tmp", "/s3-transfer-test/file.mxf"
        )

        # Check if touch command has been executed
        assert client_mock.exec_command.call_args_list[1].args == (
            "touch '/s3-transfer-test/file.mxf'",
        )

        # Check if tmp dir has not been removed
        sftp_mock.rmdir.assert_not_called()

        # Check logged message
        log_record = caplog.records[1]
        assert log_record.level == "error"
        assert (
            log_record.message
            == "SSH Error occurred when touching tmp file: No touching!"
        )
        assert log_record.tmp_filename == "/s3-transfer-test/file.mxf.part/file.mxf.tmp"

    @patch("app.helpers.transfer.build_curl_command", return_value="curl")
    def test_transfer_file_rmdir_os_error(
        self, build_curl_command_mock, transfer, caplog
    ):
        """Error when removing tmp dir."""
        stdin_mock, stdout_mock, stderr_mock = (MagicMock(), MagicMock(), MagicMock())
        # Mock the stdout result of the cURL command
        stdout_result = ["206,time: 5s,size: 1000 bytes,speed: 200b/s"]
        stdout_mock.readlines.return_value = stdout_result
        # Mock stderr to be empty
        stderr_mock.readlines.return_value = []

        client_mock = transfer.remote_client
        client_mock.exec_command.return_value = (stdin_mock, stdout_mock, stderr_mock)

        transfer.size_in_bytes = 1000
        sftp_mock = transfer.sftp
        # Mock check filesize of transferred file
        sftp_mock.stat.return_value.st_size = 1000

        sftp_mock.rmdir.side_effect = OSError("Folder doesn't exist")
        with pytest.raises(TransferException):
            transfer._transfer_file()

        # Check logged message
        log_record = caplog.records[0]
        assert log_record.level == "info"
        assert log_record.message == "Successfully cURLed tmp file"
        assert log_record.destination == "/s3-transfer-test/file.mxf.part/file.mxf.tmp"

        assert client_mock.exec_command.call_count == 2

        # Check if curl command gets called with the correct arguments
        build_curl_command_mock.assert_called_once_with(
            "/s3-transfer-test/file.mxf.part/file.mxf.tmp",
            "http://url/bucket/file.mxf",
            "domain",
        )

        # Check if tmp file is correct size
        sftp_mock.stat.assert_called_once_with(
            "/s3-transfer-test/file.mxf.part/file.mxf.tmp"
        )

        # Check if tmp file renamed
        sftp_mock.rename.assert_called_once_with(
            "/s3-transfer-test/file.mxf.part/file.mxf.tmp", "/s3-transfer-test/file.mxf"
        )

        # Check if touch command has been executed
        assert client_mock.exec_command.call_args_list[1].args == (
            "touch '/s3-transfer-test/file.mxf'",
        )

        # Check if tmp dir has been removed
        sftp_mock.rmdir.assert_called_once_with(
            "/s3-transfer-test/file.mxf.part",
        )

        # Check logged message
        log_record = caplog.records[1]
        assert log_record.level == "error"
        assert (
            log_record.message
            == "Error occurred when removing tmp folder: Folder doesn't exist"
        )
        assert log_record.tmp_folder == "/s3-transfer-test/file.mxf.part"

    @patch("time.sleep", return_value=None)
    @patch.dict(
        "app.helpers.transfer.dest_conf",
        {"free_space_percentage": "15", "file_system": "/mnt"},
    )
    def test_check_free_space(self, sleep_mock, transfer, caplog):
        """
        Check the free space twice. First there will not be enough free space.
        Then after a sleep, it will check again. The second check will return
        enough free space.
        """
        # Mock exec command
        stdin_mock, stdout_mock_no_space, stdout_mock_enough_space, stderr_mock = (
            MagicMock(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
        )
        stdout_mock_no_space.readlines.return_value = [" 95%\n"]
        stdout_mock_enough_space.readlines.return_value = [" 15%\n"]

        client_mock = transfer.remote_client

        client_mock.exec_command.side_effect = [
            (stdin_mock, stdout_mock_no_space, stderr_mock),
            (stdin_mock, stdout_mock_enough_space, stderr_mock),
        ]

        transfer._check_free_space()

        # Check executing the 'df' commands
        transfer.remote_client.exec_command.call_count == 2
        for args in transfer.remote_client.exec_command.cal_args_list:
            assert args.args == "df --output=pcent /mnt | tail -1"

        # Check time.sleep
        sleep_mock.assert_called_once_with(120)

        # Check logs
        log_record = caplog.records[0]
        assert log_record.level == "info"
        assert log_record.message == "Free space: 5%. Space needed: 15%"
        log_record = caplog.records[1]
        assert log_record.level == "info"
        assert log_record.message == "Free space: 85%. Space needed: 15%"

    @patch.dict(
        "app.helpers.transfer.dest_conf",
        {"free_space_percentage": "", "file_system": ""},
    )
    def test_check_free_space_empty_config(self, transfer, caplog):
        transfer._check_free_space()

        transfer.remote_client.exec_command.assert_not_called()

        assert not len(caplog.records)

    @patch.object(Transfer, "_init_remote_client")
    @patch.object(Transfer, "_check_free_space")
    @patch.object(Transfer, "_fetch_size")
    @patch.object(Transfer, "_prepare_target_transfer")
    @patch.object(Transfer, "_transfer_file")
    def test_transfer(
        self,
        transfer_file_mock,
        prepare_target_transfer_mock,
        fetch_size_mock,
        check_free_space_mock,
        init_remote_client_mock,
        transfer,
        caplog,
    ):
        fetch_size_mock.return_value = 100

        # Assert instance variables
        assert transfer.domain == "domain"
        assert transfer.destination_path == "/s3-transfer-test/file.mxf"
        assert transfer.dest_folder_dirname == "/s3-transfer-test"
        assert transfer.dest_file_basename == "file.mxf"
        assert transfer.dest_file_tmp_basename == "file.mxf.tmp"
        assert transfer.dest_folder_tmp_dirname == "/s3-transfer-test/file.mxf.part"
        assert (
            transfer.dest_file_tmp_full
            == "/s3-transfer-test/file.mxf.part/file.mxf.tmp"
        )

        assert transfer.source_url == "http://url/bucket/file.mxf"
        assert transfer.size_in_bytes == 0

        transfer.transfer()
        # Initialisation of the remote client
        assert init_remote_client_mock.call_count == 1
        assert transfer.remote_client.close.call_count == 1
        # Free space check
        check_free_space_mock.assert_called_once()
        # Fetch size
        fetch_size_mock.assert_called_once()
        # Prepare the target server for transferring the parts
        prepare_target_transfer_mock.assert_called_once()
        # Transfer parts
        transfer_file_mock.assert_called_once()

        # Check info log
        log_record = caplog.records[0]
        assert log_record.level == "info"
        assert (
            log_record.message
            == "Start transferring of file: http://url/bucket/file.mxf"
        )

        assert transfer.size_in_bytes == 100
