#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import functools
import threading

from pika.exceptions import AMQPConnectionError
from viaa.configuration import ConfigParser
from viaa.observability import logging

from app.helpers.message_parser import parse_validate_json, InvalidMessageException
from app.helpers.transfer import TransferException, Transfer
from app.services.rabbit import RabbitClient


class EventListener:
    def __init__(self):
        config_parser = ConfigParser()
        self.config = config_parser.app_cfg
        self.log = logging.get_logger(__name__, config=config_parser)
        self.threads = []
        try:
            self.rabbit_client = RabbitClient()
        except AMQPConnectionError as error:
            self.log.error("Connection to RabbitMQ failed.")
            raise error

    def ack_message(self, channel, delivery_tag):
        if channel.is_open:
            channel.basic_ack(delivery_tag)
        else:
            # Channel is already closed, so we can't ACK this message
            # TODO: handle properly
            pass

    def nack_message(self, channel, delivery_tag):
        if channel.is_open:
            channel.basic_nack(delivery_tag, requeue=False)
        else:
            # Channel is already closed, so we can't NACK this message
            # TODO: handle properly
            pass

    def do_work(self, channel, delivery_tag, body):
        # Parse and validate the message
        try:
            message = parse_validate_json(body)
        except InvalidMessageException as ime:
            self.log.warning(ime.message)
            cb_nack = functools.partial(self.nack_message, channel, delivery_tag)
            self.rabbit_client.connection.add_callback_threadsafe(cb_nack)
            return

        # Start the transfer
        try:
            Transfer(message).transfer()
        except (TransferException, OSError):
            self.log.error("Transfer failed")
            cb_nack = functools.partial(self.nack_message, channel, delivery_tag)
            self.rabbit_client.connection.add_callback_threadsafe(cb_nack)
        else:
            cb_ack = functools.partial(self.ack_message, channel, delivery_tag)
            self.rabbit_client.connection.add_callback_threadsafe(cb_ack)

    def handle_message(self, channel, method, properties, body):
        """Main method that will handle the incoming messages.

        The transfer potentially takes a long time to finish. As this is
        blocking the RabbitMQ I/O loop, this might result in a heartbeat
        timeout and the rabbit broker closing the connection on its end.

        So, we run the file transfer in a separate thread making sure the
        RabbitMQ I/O loop is not blocked.

        That thread will be appended to a list, in order to be able to wait
        for all threads to finish in the case consuming is stopped.
        """
        self.log.debug(f"Incoming message: {body}")

        # Clean up the list of threads, so it doesn't keep appending
        for t in self.threads:
            if not t.is_alive():
                t.handled = True
        self.threads = [t for t in self.threads if not t.handled]

        thread = threading.Thread(
            target=self.do_work, args=(channel, method.delivery_tag, body)
        )
        thread.handled = False
        thread.start()
        self.threads.append(thread)

    def exit_gracefully(self, signum, frame):
        """Stop consuming queue but finish current tasks/messages."""
        self.log.info(
            "Received SIGTERM. Waiting for last transfer to finish and then stops."
        )
        self.rabbit_client.stop_consuming()

    def start(self):
        # Start listening for incoming messages
        self.log.info("Start to listen for incoming transfer messages...")
        self.rabbit_client.listen(self.handle_message)
        # Wait for remaining threads to join after consuming.
        for thread in self.threads:
            thread.join()
        # Ensure callback (n)acks are send
        self.rabbit_client.connection.process_data_events()
        # Close the RabbitMQ connection
        self.rabbit_client.connection.close()
