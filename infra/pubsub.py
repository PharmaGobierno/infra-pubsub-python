from __future__ import annotations

from json import dumps
from os import getenv
from typing import Optional

from google.api_core.retry import Retry
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.types import BatchSettings, PublisherOptions
from google.oauth2.service_account import Credentials


class PubsubConnectionConf:
    __version__ = "1.0.0"
    _DEFAULT_BATCH_MESSAGES_ = 100
    _DEFAULT_MESSAGE_SIZE_ = 10**6

    def __init__(
        self,
        *,
        service_account_file: Optional[str],
        enable_message_ordering: bool = False,
        max_messages: Optional[int] = None,
        max_bytes: Optional[int] = None,
    ) -> None:
        self.credentials: Optional[Credentials] = None
        if service_account_file:
            self.credentials = Credentials.from_service_account_file(
                service_account_file,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
        self.project_id = self.__get_default_env("GOOGLE_CLOUD_PROJECT")
        self.__set_batch_settings(max_messages, max_bytes)
        self.__set_publisher_options(enable_message_ordering)

    def __get_default_env(self, name: str) -> str:
        """
        Look for values in enviroment file.
        :param name: The name to look in env
        :type name: str
        :return: Value given the name
        :rtype: str
        """
        value: Optional[str] = getenv(name)
        if value is None:
            raise ValueError(f"The default value for {name} was not found in ENV FILE")
        return value

    def __set_batch_settings(
        self, max_messages: Optional[int] = None, max_bytes: Optional[int] = None
    ):
        self.bach_settings = BatchSettings(
            max_bytes=(max_bytes or self._DEFAULT_MESSAGE_SIZE_),
            max_messages=(max_messages or self._DEFAULT_BATCH_MESSAGES_),
        )

    def __set_publisher_options(self, enable_message_ordering: bool):
        self.publisher_options = PublisherOptions(
            enable_message_ordering=enable_message_ordering, retry=Retry()
        )


class PubsubManager:
    __version__ = "1.0.0"

    def __init__(
        self,
        connection_conf: PubsubConnectionConf,
    ) -> None:
        self._connection_conf = connection_conf
        self.__init_client()

    def __init_client(self):
        self._client = pubsub_v1.PublisherClient(
            batch_settings=self._connection_conf.bach_settings,
            publisher_options=self._connection_conf.publisher_options,
            credentials=self._connection_conf.credentials,
        )

    def __get_topic_path(self, topic_name: str) -> str:
        """
        Formats the topic into Cloud PubSub's format.

        :param topic_name: Target topic name
        :type topic_name: str
        :return: A Cloud PubSub formatted topic
        :rtype: str
        """
        resulting_topic = self._client.topic_path(
            self._connection_conf.project_id, topic_name
        )
        return resulting_topic

    def publish(
        self,
        topic: str,
        message: object,
        attributes: Optional[dict] = None,
        ordering_key: Optional[str] = None,
    ) -> Optional[str]:
        """
        Publishes a Pubsub message to a topic.

        :param topic: Cloud PubSub topic
        :type topic: str
        :param message: Message object
        :param attributes: Metadata attributes
        :type attributes: object
        :param ordering_key: Unique key to enable message ordering
        :type ordering_key: str
        """
        _params: dict = {}
        if attributes:
            _params.update(attributes)

        if ordering_key:
            _params.update({"ordering_key": ordering_key})

        _params.update(
            {
                "project": self._connection_conf.project_id,
                "topic": self.__get_topic_path(topic),
                "data": str.encode(dumps(message)),
            }
        )

        future = self._client.publish(**_params)
        return future.result()
