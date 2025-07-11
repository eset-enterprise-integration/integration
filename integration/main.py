import asyncio
import logging
import time
import typing as t
from datetime import datetime, timezone

from aiohttp import ClientSession

from integration.models import Config, EnvVariables, TokenStorage
from integration.utils import LastDataTimeHandler, RequestSender, TokenProvider, TransformerData


class ServiceClient:
    def __init__(self) -> None:
        self.config = self._get_config()
        self.env_vars = self._get_env_vars()
        self.request_sender = self._get_request_sender()
        self.token_provider = self._get_token_provider()
        self.transformer_data = self._get_transformer_data()
        self._session: t.Optional[ClientSession] = None
        self._lock: t.Optional[asyncio.Lock] = None

    def _get_config(self) -> Config:
        return Config("", "")

    def _get_env_vars(self) -> EnvVariables:
        return EnvVariables()

    def _get_request_sender(self) -> RequestSender:
        return RequestSender(self.config, self.env_vars)

    def _get_token_provider(self) -> TokenProvider:
        return TokenProvider(TokenStorage(), self.request_sender, self.env_vars, self.config.buffer)

    def _get_last_data_time_handler(self, data_source: str) -> LastDataTimeHandler:
        return LastDataTimeHandler(data_source, self.env_vars.interval)

    def _get_transformer_data(self) -> TransformerData:
        return TransformerData(self.env_vars)

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def run(self) -> None:
        self._session = ClientSession()
        self._lock = asyncio.Lock()
        start_time = time.time()
        try:
            await asyncio.gather(
                self._process_integration("EP", start_time),
                self._process_integration("EI", start_time),
                self._process_integration("ECOS", start_time),
                self._process_integration("INCIDENTS", start_time),
            )
        except Exception as e:
            logging.error("Unexpected error happened", exc_info=e)
            raise e
        finally:
            await self.close()

    def _validate_if_run_instance(self, data_source: str) -> bool:
        if data_source == "EP" and self.env_vars.ep_instance == "yes" and self.env_vars.ei_instance == "no":
            return True
        if data_source == "EI" and self.env_vars.ei_instance == "yes":
            return True
        if data_source == "ECOS" and self.env_vars.ecos_instance == "yes":
            return True
        if (
            data_source == "INCIDENTS"
            and (self.env_vars.ep_instance == "yes" or self.env_vars.ei_instance == "yes")
            and self._validate_if_run_incidents()
        ):
            return True
        return False

    def _validate_if_run_incidents(self) -> bool:
        return True

    def _validate_if_run_instance_old_version(self, data_source: str) -> bool:
        return False

    async def _process_integration(self, data_source: str, start_time: float) -> None:
        logging.info(f"Running process integration for {data_source}")
        if not any(
            [self._validate_if_run_instance(data_source), self._validate_if_run_instance_old_version(data_source)]
        ):
            logging.info(f"Validate if run {data_source} returned False")
            return

        last_data_time_handler = self._get_last_data_time_handler(data_source)
        next_page_token: t.Optional[str] = None
        cur_ld_time: t.Optional[str] = None
        max_duration: int = self.env_vars.interval * 60

        data_source, last_data_time_handler = await self._old_version_check(data_source, last_data_time_handler)
        endp = self.config.data_sources.get(data_source).get("endpoint")  # type: ignore

        await self._run_process_integration(
            cur_ld_time, data_source, endp, last_data_time_handler, max_duration, next_page_token, start_time
        )

    async def _old_version_check(
        self, data_source: str, last_data_time_handler: LastDataTimeHandler
    ) -> tuple[str, LastDataTimeHandler]:
        return data_source, last_data_time_handler

    async def _run_process_integration(
        self,
        cur_ld_time: t.Optional[str],
        data_source: str,
        endp: str,
        last_data_time_handler: LastDataTimeHandler,
        max_duration: int,
        next_page_token: t.Optional[str],
        start_time: float,
    ) -> None:
        while next_page_token != "" and (time.time() - start_time) < (max_duration - 30):
            response_data = await self._call_service(last_data_time_handler, next_page_token, data_endpoint=endp)
            next_page_token = response_data.get("nextPageToken") if response_data else ""

            if (
                response_data
                and any(response_data.get(v) for v in ("detections", "detectionGroups", "incidents"))
                and (time.time() - start_time) < (max_duration - 15)
            ):
                cur_ld_time, successful_data_upload = await self.transformer_data.send_integration_data(  # type: ignore
                    response_data, cur_ld_time, endp
                )
                next_page_token = "" if successful_data_upload is False else next_page_token
                assert self._lock
                async with self._lock:
                    await last_data_time_handler.update_last_data_time(cur_ld_time, data_source)

    async def _call_service(
        self,
        last_data_time_handler: LastDataTimeHandler,
        next_page_token: t.Optional[str],
        page_size: int = 100,
        data_endpoint: str = "",
    ) -> t.Optional[dict[str, t.Any]]:
        logging.info(f"Service call initiated")

        if not self.token_provider.token.access_token or datetime.now(timezone.utc) > self.token_provider.token.expiration_time:  # type: ignore
            assert self._session and self._lock
            async with self._lock:
                await self.token_provider.get_token(self._session)

        try:
            if (
                self.token_provider.token.expiration_time
                and datetime.now(timezone.utc) < self.token_provider.token.expiration_time
            ):
                data = await self.request_sender.send_request(
                    self.request_sender.send_request_get,
                    self._session,  # type: ignore
                    {
                        "Authorization": f"Bearer {self.token_provider.token.access_token}",
                        "Content-Type": "application/json",
                        "3rd-integration": self.config.integration_name,
                        "Version": self.config.version,
                    },
                    last_data_time_handler.last_data_time,
                    next_page_token,
                    page_size,
                    data_endpoint,
                )
                is_obtained = (
                    True if data and any(data.get(v) for v in ("detections", "detectionGroups", "incidents")) else False
                )
                logging.info(
                    f"Service call {data_endpoint} response data is {'obtained' if is_obtained else f'empty: {data}'}"
                )
                return data

            logging.info("Service not called due to missing token.")
        except Exception as e:
            logging.error(f"Error in running service call: {e}")

        return None
