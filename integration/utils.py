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
from integration.models_detections import Detection, Detections


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
            return await response.json()

    async def send_request_get(
        self,
        session: ClientSession,
        headers: t.Optional[dict[str, t.Any]],
        last_detection_time: str,
        next_page_token: t.Optional[str],
        page_size: int,
        data_endpoint: str,
    ) -> t.Union[dict[str, t.Union[str, int]], t.Any]:
        logging.info("Sending service request")

        async with session.get(
            self.env_vars.detections_url + data_endpoint,
            headers=headers,
            params=self._prepare_get_request_params(last_detection_time, next_page_token, page_size),
        ) as response:
            return await response.json()

    def _prepare_get_request_params(
        self, last_detection_time: str, next_page_token: t.Optional[str], page_size: int = 100
    ) -> dict[str, t.Any]:
        params = {"pageSize": page_size}
        if next_page_token not in ["", None]:
            params["pageToken"] = next_page_token  # type: ignore[assignment]
        if last_detection_time:
            params["startTime"] = last_detection_time  # type: ignore[assignment]

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


class TransformerDetections:
    def __init__(self, env_vars: EnvVariables) -> None:
        self.env_vars = env_vars

    async def send_integration_detections(
        self, detections: t.Optional[dict[str, t.Any]], last_detection: t.Optional[str]
    ) -> t.Optional[tuple[t.Optional[str], bool]]:
        validated_detections = self._validate_detections_data(detections)
        if not validated_detections:
            return last_detection, False
        return await self._send_data_to_destination(validated_detections, last_detection)

    async def _send_data_to_destination(
        self, validated_data: t.List[dict[str, t.Any]], last_detection: t.Optional[str]
    ) -> t.Optional[tuple[t.Optional[str], bool]]:
        pass

    def _validate_detections_data(
        self, response_data: t.Optional[dict[str, t.Any]]
    ) -> t.Optional[list[dict[str, t.Any]]]:
        if not response_data:
            logging.info("No new detections")
            return None

        response_data["detections"] = (
            response_data.pop("detectionGroups") if "detectionGroups" in response_data else response_data["detections"]
        )
        try:
            validated_data = Detections.model_validate(response_data)
            self._update_time_generated(validated_data.detections)
            return validated_data.model_dump().get("detections")

        except ValidationError as e:
            logging.error(e)
            validated_detections = []

            for detection in response_data.get("detections"):  # type: ignore
                try:
                    validated_detections.append(Detection.model_validate(detection))
                except ValidationError as e:
                    logging.error(e)

            self._update_time_generated(validated_detections)
            return [d.model_dump() for d in validated_detections]

    def _update_time_generated(self, validated_data: t.List[Detection]) -> None:
        utc_now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        for data in validated_data:
            data.TimeGenerated = utc_now


class LastDetectionTimeHandler:
    def __init__(self, data_source: str, interval: int) -> None:
        self.last_detection_time = self.get_last_occur_time(data_source, interval)

    def prepare_date_plus_timedelta(self, cur_last_detection_time: str) -> str:
        return (
            datetime.strptime(
                self.transform_date_with_miliseconds_to_second(cur_last_detection_time), "%Y-%m-%dT%H:%M:%SZ"
            )
            + timedelta(seconds=1)
        ).isoformat() + "Z"

    def transform_date_with_miliseconds_to_second(self, cur_last_detection_time: str) -> str:
        return cur_last_detection_time if len(cur_last_detection_time) == 20 else cur_last_detection_time[:-5] + "Z"

    def get_last_occur_time(self, data_source: t.Optional[str] = None, interval: int = 5) -> t.Optional[str]:
        pass

    async def update_last_detection_time(self, cur_ld_time: t.Optional[str], data_source: str) -> None:
        pass
