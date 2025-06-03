import os
import typing as t
from dataclasses import dataclass, field
from datetime import datetime

from dotenv import load_dotenv


@dataclass
class TokenStorage:
    __access_token: t.Optional[str] = field(default=None, init=False)
    __refresh_token: t.Optional[str] = field(default=None, init=False)
    __expiration_time: t.Optional[datetime] = field(default=None, init=False)

    @property
    def access_token(self) -> t.Optional[str]:
        return self.__access_token

    @access_token.setter
    def access_token(self, value: str) -> None:
        self.__access_token = value

    @property
    def refresh_token(self) -> t.Optional[str]:
        return self.__refresh_token

    @refresh_token.setter
    def refresh_token(self, value: str) -> None:
        self.__refresh_token = value

    @property
    def expiration_time(self) -> t.Optional[datetime]:
        return self.__expiration_time

    @expiration_time.setter
    def expiration_time(self, value: datetime) -> None:
        self.__expiration_time = value

    def to_dict(self) -> dict[str, t.Any]:
        return {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "expiration_time": self.expiration_time,
        }


class Config:
    def __init__(self, integration_name: str, version: str) -> None:
        self.max_retries: int = 3
        self.retry_delay: float = 60
        self.requests_timeout = 3600
        self.buffer = 5
        self.data_sources: dict[str, t.Any] = {
            "EI": {"endpoint": "/v2/detection-groups"},
            "EP": {"endpoint": "/v1/detections"},
            "ECOS": {"endpoint": "/v2/detections"},
            "INCIDENTS": {"endpoint": "/v2/incidents"},
        }
        self.integration_name: str = integration_name
        self.version: str = version


class EnvVariables:
    def __init__(self) -> None:
        load_dotenv()
        self.__username: t.Optional[str] = os.getenv("USERNAME_INTEGRATION")
        self.__password: t.Optional[str] = os.getenv("PASSWORD_INTEGRATION")
        self.interval: int = int(os.getenv("INTERVAL", 5)) if int(os.getenv("INTERVAL", 5)) >= 3 else 3

        self.ep_instance: str = os.getenv("EP_INSTANCE", "").lower()
        self.ei_instance: str = os.getenv("EI_INSTANCE", "").lower()
        self.ecos_instance: str = os.getenv("ECOS_INSTANCE", "").lower()

        region = os.getenv("INSTANCE_REGION", "eu")
        self.oauth_url: str = f"https://{region}.business-account.iam.eset.systems"
        self.data_url: str = f"https://{region}.incident-management.eset.systems"

    @property
    def username(self) -> t.Optional[str]:
        return self.__username

    @property
    def password(self) -> t.Optional[str]:
        return self.__password
