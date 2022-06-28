#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import pytest
from unittest.mock import MagicMock, patch

from paramiko import SSHException

from app.helpers.transfer import (
    build_curl_command,
    calculate_filename_part,
    calculate_ranges,
    Transfer,
    TransferException,
    TransferPartException,
)


@pytest.mark.parametrize(
    "size, number_parts, expected",
    [
        (1303, 4, ["0-326", "327-652", "653-977", "978-1303"]),
        (6, 6, ["0-1", "2-2", "3-3", "4-4", "5-5", "6-6"]),
        (1000, 1, ["0-1000"]),
    ],
)
def test_calculate_ranges(size, number_parts, expected):
    assert calculate_ranges(size, number_parts) == expected


@pytest.mark.parametrize(
    "size, number_parts, side_effect, message",
    [
        (1000, 0, ZeroDivisionError, "division by zero"),
        (2, 4, ValueError, "Amount of parts '4' is greater than the size '2'"),
    ],
)
def test_calculate_ranges_error(size, number_parts, side_effect, message):
    with pytest.raises(side_effect) as e:
        calculate_ranges(size, number_parts)
    assert str(e.value) == message


def test_build_curl_command():
    dest = "dest file"
    src = "source file"
    domain = "S3 domain"
    r = "0-100"
    w_params = "%{http_code},time: %{time_total}s,size: %{size_download} bytes,speed: %{speed_download}b/s"
    curl_command = build_curl_command(dest, src, domain, r)
    assert (
        curl_command
        == f"curl -w '{w_params}' -L -H 'host: {domain}' -H 'range: bytes={r}' -r {r} -S -s -o '{dest}' '{src}'"
    )


def test_calculate_filename_part():
    assert calculate_filename_part("file.mxf", 0) == "file.mxf.part0"


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

    @patch("app.helpers.transfer.build_curl_command", return_value="curl")
    @patch("app.helpers.transfer.SSHClient")
    def test_transfer_part(
        self, ssh_client_mock, build_curl_command_mock, transfer, caplog
    ):
        """Successful transfer of a part."""
        stdin_mock, stdout_mock, stderr_mock = (MagicMock(), MagicMock(), MagicMock())
        # Mock the stdout result of the cURL command
        stdout_result = ["206,time: 5s,size: 1000 bytes,speed: 200b/s"]
        stdout_mock.readlines.return_value = stdout_result
        # Mock stderr to be empty
        stderr_mock.readlines.return_value = []

        # Mock exec command
        client_mock = ssh_client_mock().__enter__()
        client_mock.exec_command.return_value = (stdin_mock, stdout_mock, stderr_mock)

        transfer._transfer_part("dest", "0-100")

        assert client_mock.set_missing_host_key_policy.call_count == 1
        assert client_mock.connect.call_count == 1
        assert client_mock.connect.call_args.args == ("ssh_host",)
        assert client_mock.connect.call_args.kwargs == {
            "port": 22,
            "username": "ssh_user",
            "password": "ssh_pass",
        }

        # Check if curl command gets called with the correct arguments
        build_curl_command_mock.assert_called_once_with(
            "dest", "http://url/bucket/file.mxf", "domain", "0-100"
        )

        assert client_mock.exec_command() == (stdin_mock, stdout_mock, stderr_mock)
        assert "Successfully cURLed part" in caplog.messages

    @patch("app.helpers.transfer.SSHClient")
    @patch("time.sleep", MagicMock())
    def test_transfer_part_status_code(self, ssh_client_mock, transfer, caplog):
        """HTTP error occurs when transferring a part."""
        stdin_mock, stdout_mock, stderr_mock = (MagicMock(), MagicMock(), MagicMock())
        # Mock the stdout result of the cURL command
        stdout_result = ["416,time: 5s,size: 1000 bytes,speed: 200b/s"]
        stdout_mock.readlines.return_value = stdout_result
        # Mock stderr to be empty
        stderr_mock.readlines.return_value = []

        # Mock exec command
        client_mock = ssh_client_mock().__enter__()
        client_mock.exec_command.return_value = (stdin_mock, stdout_mock, stderr_mock)
        with pytest.raises(TransferPartException):
            transfer._transfer_part("dest", "0-100")

        assert client_mock.set_missing_host_key_policy.call_count == 3
        assert client_mock.connect.call_count == 3
        assert client_mock.connect.call_args.args == ("ssh_host",)
        assert client_mock.connect.call_args.kwargs == {
            "port": 22,
            "username": "ssh_user",
            "password": "ssh_pass",
        }
        assert client_mock.exec_command() == (stdin_mock, stdout_mock, stderr_mock)
        assert (
            "Error occurred when cURLing part with status code: 416" in caplog.messages
        )

    @patch("app.helpers.transfer.SSHClient")
    @patch("time.sleep", MagicMock())
    def test_transfer_part_stderr(self, ssh_client_mock, transfer, caplog):
        """Transferring a part resulting in stderr output."""
        stdin_mock, stdout_mock, stderr_mock = (MagicMock(), MagicMock(), MagicMock())
        # Mock the stderr result of the cURL command
        stderr_result = ["Error"]
        stderr_mock.readlines.return_value = stderr_result

        # Mock exec command
        client_mock = ssh_client_mock().__enter__()
        client_mock.exec_command.return_value = (stdin_mock, stdout_mock, stderr_mock)
        with pytest.raises(TransferPartException):
            transfer._transfer_part("dest", "0-100")
        assert "Error occurred when cURLing part: ['Error']" in caplog.messages

    @patch("app.helpers.transfer.SSHClient")
    @patch("time.sleep", MagicMock())
    def test_transfer_part_ssh_exception(self, ssh_client_mock, transfer, caplog):
        """SSH Exception occurs when connecting."""
        client_mock = ssh_client_mock().__enter__()
        client_mock.connect.side_effect = SSHException("Connection error")
        with pytest.raises(TransferPartException):
            transfer._transfer_part("dest", "0-100")
        assert not client_mock.exec_command.call_count
        assert (
            "SSH Error occurred when cURLing part: Connection error" in caplog.messages
        )

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

    @patch("app.helpers.transfer.Transfer._transfer_part")
    @patch("app.helpers.transfer.calculate_ranges", return_value=["0-1", "1-2"])
    def test_transfer_parts(
        self, calculate_ranges_mock, transfer_part_mock, transfer, caplog
    ):
        """Transfer parts successfully."""
        transfer._transfer_parts()

        log_records = caplog.records
        assert len(log_records) == 2
        for log_record in log_records:
            assert log_record.level == "debug"

        assert (
            "Thread started for: /s3-transfer-test/file.mxf.part/file.mxf.part0"
            in caplog.messages
        )
        assert (
            "Thread started for: /s3-transfer-test/file.mxf.part/file.mxf.part1"
            in caplog.messages
        )

        assert transfer_part_mock.call_count == 2
        call_args = [call_args.args for call_args in transfer_part_mock.call_args_list]
        assert (
            "/s3-transfer-test/file.mxf.part/file.mxf.part1",
            "1-2",
        ) in call_args

        assert (
            "/s3-transfer-test/file.mxf.part/file.mxf.part0",
            "0-1",
        ) in call_args

    @patch("app.helpers.transfer.build_assemble_command", return_value="cat")
    def test_assemble_parts(self, build_assemble_command_mock, transfer, caplog):
        """Successfully assemble the parts."""
        stdin_mock, stdout_mock, stderr_mock = (MagicMock(), MagicMock(), MagicMock())

        # Mock exec command
        client_mock = transfer.remote_client
        client_mock.exec_command.return_value = (stdin_mock, stdout_mock, stderr_mock)

        transfer.size_in_bytes = 1000
        sftp_mock = transfer.sftp
        # Mock check filesize of transferred file
        sftp_mock.stat.return_value.st_size = 1000

        transfer._assemble_parts()

        # Check logged message
        log_record = caplog.records[0]
        assert log_record.level == "info"
        assert log_record.message == "Start assembling the parts"
        assert log_record.destination == "/s3-transfer-test/file.mxf"

        assert client_mock.exec_command.call_count == 2

        # Check call of build assemble command
        build_assemble_command_mock.assert_called_once_with(
            "/s3-transfer-test/file.mxf.part", "file.mxf", 4
        )

        # Check if build command has executed
        assert client_mock.exec_command.call_args_list[0].args == ("cat",)

        # Check if changed into tmp dir
        sftp_mock.chdir.assert_called_once_with(
            "/s3-transfer-test/file.mxf.part",
        )
        # Check if tmp file is correct size
        sftp_mock.stat.assert_called_once_with(
            "file.mxf.tmp",
        )

        # Check if tmp file renamed
        sftp_mock.rename.assert_called_once_with(
            "/s3-transfer-test/file.mxf.part/file.mxf.tmp", "/s3-transfer-test/file.mxf"
        )

        # Check if touch command has been executed
        assert client_mock.exec_command.call_args_list[1].args == (
            "touch '/s3-transfer-test/file.mxf'",
        )

        # Check if remove parts command has been executed for each part
        assert sftp_mock.remove.call_count == 4
        for idx, cargs in enumerate(sftp_mock.remove.call_args_list):
            assert cargs.args == (f"file.mxf.part{idx}",)

        # Check if tmp dir has been removed
        sftp_mock.rmdir.assert_called_once_with(
            "/s3-transfer-test/file.mxf.part",
        )

        # Check logged message
        log_record = caplog.records[1]
        assert log_record.level == "info"
        assert log_record.message == "File successfully transferred"
        assert log_record.destination == "/s3-transfer-test/file.mxf"

    @patch("app.helpers.transfer.build_assemble_command", return_value="cat")
    def test_assemble_parts_different_size(
        self, build_assemble_command_mock, transfer, caplog
    ):
        """Assembled file has incorrect file size."""
        stdin_mock, stdout_mock, stderr_mock = (MagicMock(), MagicMock(), MagicMock())

        # Mock exec command
        client_mock = transfer.remote_client
        client_mock.exec_command.return_value = (stdin_mock, stdout_mock, stderr_mock)

        transfer.size_in_bytes = 1000
        sftp_mock = transfer.sftp
        # Mock check filesize of transferred file
        sftp_mock.stat.return_value.st_size = 500

        with pytest.raises(TransferException):
            transfer._assemble_parts()

        # Tmp file should not be renamed
        sftp_mock.rename.assert_not_called()

        # Check error log
        log_record = caplog.records[-1]
        assert log_record.level == "error"
        assert log_record.message == "Size of assembled file: 500, expected size: 1000"
        assert log_record.source_url == "http://url/bucket/file.mxf"
        assert log_record.destination_basename == "file.mxf.tmp"

    @patch("app.helpers.transfer.build_assemble_command", return_value="cat")
    def test_assemble_parts_os_error(
        self, build_assemble_command_mock, transfer, caplog
    ):
        """An OSError occurred when assembling."""
        stdin_mock, stdout_mock, stderr_mock = (MagicMock(), MagicMock(), MagicMock())

        # Mock exec command
        client_mock = transfer.remote_client
        client_mock.exec_command.return_value = (stdin_mock, stdout_mock, stderr_mock)

        sftp_mock = transfer.sftp
        # Checking filesize returns OSError
        sftp_mock.stat.side_effect = OSError("error")

        with pytest.raises(TransferException):
            transfer._assemble_parts()

        # Tmp file should not be renamed
        sftp_mock.rename.assert_not_called()

        # Check error log
        log_record = caplog.records[-1]
        assert log_record.level == "error"
        assert log_record.message == "Error occurred when assembling parts: error"

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
    @patch.object(Transfer, "_transfer_parts")
    @patch.object(Transfer, "_assemble_parts")
    def test_transfer(
        self,
        assemble_parts_mock,
        transfer_parts_mock,
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
        assert transfer.source_url == "http://url/bucket/file.mxf"
        assert transfer.size_in_bytes == 0

        transfer.transfer()
        # Initialisation of the remote client
        assert init_remote_client_mock.call_count == 2
        assert transfer.remote_client.close.call_count == 2
        # Free space check
        check_free_space_mock.assert_called_once()
        # Fetch size
        fetch_size_mock.assert_called_once()
        # Prepare the target server for transferring the parts
        prepare_target_transfer_mock.assert_called_once()
        # Transfer parts
        transfer_parts_mock.assert_called_once()
        # Assemble parts
        assemble_parts_mock.assert_called_once()

        # Check info log
        log_record = caplog.records[0]
        assert log_record.level == "info"
        assert (
            log_record.message
            == "Start transferring of file: http://url/bucket/file.mxf"
        )

        assert transfer.size_in_bytes == 100
