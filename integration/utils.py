import asyncio
import logging
import typing as t
import urllib.parse
from datetime import datetime, timedelta, timezone

from aiohttp import ClientSession
from aiohttp.client_exceptions import ClientResponseError
from pydantic import ValidationError

from integration.exceptions import (
    AuthenticationException,
    InvalidCredentialsException,
    MissingCredentialsException,
    TokenRefreshException,
)
from integration.models import Config, EnvVariables, TokenStorage
from integration.models_data import Detection, Detections, Incident, Incidents


class RequestSender:
    def __init__(self, config: Config, env_vars: EnvVariables):
        self.config = config
        self.env_vars = env_vars

    async def send_request(
        self,
        send_request_fun: t.Union[
            t.Callable[
                [ClientSession, t.Optional[dict[str, t.Any]], str, t.Optional[str], int, str],
                t.Coroutine[t.Any, t.Any, t.Union[dict[str, t.Union[str, int]], t.Any]],
            ],
            t.Callable[
                [ClientSession, t.Optional[dict[str, t.Any]], t.Optional[str]],
                t.Coroutine[t.Any, t.Any, t.Union[dict[str, t.Union[str, int]], t.Any]],
            ],
        ],
        session: ClientSession,
        headers: t.Optional[dict[str, t.Any]] = None,
        *data: t.Any,
    ) -> t.Optional[dict[str, t.Union[str, int]]]:
        retries = 0

        while retries < self.config.max_retries:
            try:
                return await send_request_fun(session, headers, *data)

            except ClientResponseError as e:
                if e.headers:
                    logging.info(f"Request-ID: {e.headers.get('Request-ID')}")

                if e.status in [400, 401, 403]:
                    raise AuthenticationException(status=e.status, message=e.message)
                if e.status == 404:
                    logging.info(f"Endpoint not found.")
                    return None

                retries += 1
                logging.error(
                    f"Exception: {e.status} {e.message}. Request failed. "
                    f"Request retry attempt: {retries}/{self.config.max_retries}"
                )
                await asyncio.sleep(self.config.retry_delay)
        return None

    async def send_request_post(
        self, session: ClientSession, headers: t.Optional[dict[str, t.Any]], grant_type: t.Optional[str]
    ) -> t.Union[dict[str, t.Union[str, int]], t.Any]:
        logging.info("Sending token request")

        async with session.post(
            url=f"{self.env_vars.oauth_url}/oauth/token",
            headers=headers,
            data=urllib.parse.quote(f"grant_type={grant_type}", safe="=&/"),
            timeout=self.config.requests_timeout,
        ) as response:
            response_json = await response.json()
            if response.status >= 400:
                logging.info(f"Response status: {response.status} Response text: {response_json}")
                response.raise_for_status()
            return response_json

    async def send_request_get(
        self,
        session: ClientSession,
        headers: t.Optional[dict[str, t.Any]],
        last_data_time: str,
        next_page_token: t.Optional[str],
        page_size: int,
        data_endpoint: str,
    ) -> t.Union[dict[str, t.Union[str, int]], t.Any]:
        logging.info("Sending service request")

        async with session.get(
            self.env_vars.data_url + data_endpoint,
            headers=headers,
            params=self._prepare_get_request_params(last_data_time, next_page_token, page_size, data_endpoint),
        ) as response:
            response_json = await response.json()
            if response.status >= 400:
                logging.info(
                    f"Response status: {response.status} Endpoint: {data_endpoint}"
                )
                response.raise_for_status()
            return response_json

    def _prepare_get_request_params(
        self, last_data_time: str, next_page_token: t.Optional[str], page_size: int = 100, data_endpoint: str = ""
    ) -> dict[str, t.Any]:
        params: dict[str, t.Any]= {"pageSize": page_size}
        if next_page_token not in ["", None]:
            params["pageToken"] = next_page_token
        if last_data_time:
            if "incidents" in data_endpoint:
                params["filter"] = f'incident.create_time >= timestamp("{last_data_time}")'
            else:
                params["startTime"] = last_data_time

        return params


class TokenProvider:
    def __init__(
        self, token: TokenStorage, requests_sender: RequestSender, env_vars: EnvVariables, buffer: int
    ) -> None:
        self.token = token
        self.requests_sender = requests_sender
        self.buffer = buffer
        self.env_vars = env_vars

    async def get_token(self, session: ClientSession) -> None:
        if not self.token.access_token or datetime.now(timezone.utc) > self.token.expiration_time:  # type: ignore
            logging.info("Getting token")

            if not self.token.access_token and (not self.env_vars.username or not self.env_vars.password):
                raise MissingCredentialsException()

            grant_type = (
                f"refresh_token&refresh_token={self.token.refresh_token}"
                if self.token.access_token
                else f"password&username={self.env_vars.username}&password={self.env_vars.password}"
            )

            try:
                response = await self.requests_sender.send_request(
                    self.requests_sender.send_request_post,
                    session,
                    {
                        "Content-type": "application/x-www-form-urlencoded",
                        "3rd-integration": self.requests_sender.config.integration_name,
                    },
                    grant_type,
                )
            except AuthenticationException as e:
                if not self.token.access_token:
                    raise InvalidCredentialsException(e)
                else:
                    self.manage_token_refresh_issue()
                    raise TokenRefreshException(e)

            if response:
                self.set_token_params_locally(response)
                logging.info("Token obtained successfully")

    def set_token_params_locally(self, response: t.Dict[str, t.Union[str, int]]) -> None:
        self.token.access_token = t.cast(str, response["access_token"])
        self.token.refresh_token = t.cast(str, response["refresh_token"])
        self.token.expiration_time = datetime.now(timezone.utc) + timedelta(
            seconds=int(response["expires_in"]) - self.buffer
        )

    def manage_token_refresh_issue(self) -> None:
        pass


class TransformerData:
    def __init__(self, env_vars: EnvVariables) -> None:
        self.env_vars = env_vars

    async def send_integration_data(
        self, data: t.Optional[dict[str, t.Any]], last_data_time: t.Optional[str], endp: str
    ) -> t.Optional[tuple[t.Optional[str], bool]]:
        data_model: type[t.Union[Detections, Incidents]] = Incidents if "incidents" in endp else Detections
        validated_data = self._validate_data(data, data_model)

        if not validated_data:
            return last_data_time, False
        return await self._send_data_to_destination(validated_data, last_data_time, endp)

    async def _send_data_to_destination(
        self, validated_data: t.List[dict[str, t.Any]], last_data_time: t.Optional[str], endp: str = ""
    ) -> t.Optional[tuple[t.Optional[str], bool]]:
        pass

    def _update_time_generated(self, validated_data: t.List[Detection | Incident]) -> None:
        utc_now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        for data in validated_data:
            data.TimeGenerated = utc_now

    def _validate_data(
        self, response_data: t.Optional[dict[str, t.Any]], data_model: type[Detections | Incidents]
    ) -> t.Optional[list[dict[str, t.Any]]]:
        if not response_data:
            logging.info("No new integration data")
            return None

        if "detectionGroups" in response_data:
            response_data["detections"] = response_data.pop("detectionGroups")

        data_key = "incidents" if data_model == Incidents else "detections"

        try:
            validated_data = data_model.model_validate(response_data)
            self._update_time_generated(getattr(validated_data, data_key, []))
            return validated_data.model_dump().get(data_key)

        except ValidationError as e:
            logging.error(e)
            validated_data_list = []
            single_data_model: type[t.Union[Detection, Incident]] = Detection if data_model == Detections else Incident

            for data in response_data.get(data_key, []):
                try:
                    validated_data_list.append(single_data_model.model_validate(data))
                except ValidationError as e:
                    logging.error(e)

            self._update_time_generated(validated_data_list)
            return [d.model_dump() for d in validated_data_list]


class LastDataTimeHandler:
    def __init__(self, data_source: str, interval: int) -> None:
        self.last_data_time = self.get_last_data_time(data_source, interval)

    def prepare_date_plus_timedelta(self, cur_last_data_time: str) -> str:
        return (self.map_time_to_seconds_precision(cur_last_data_time) + timedelta(seconds=1)).isoformat() + "Z"

    def map_time_to_seconds_precision(self, date: str) -> datetime:
        return datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ" if "." in date else "%Y-%m-%dT%H:%M:%SZ").replace(
            microsecond=0
        )

    def get_last_data_time(self, data_source: t.Optional[str] = None, interval: int = 5) -> t.Optional[str]:
        pass

    async def update_last_data_time(self, cur_ld_time: t.Optional[str], data_source: str) -> None:
        pass
