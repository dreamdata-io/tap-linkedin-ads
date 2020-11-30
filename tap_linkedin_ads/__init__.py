#!/usr/bin/env python3

# type: ignore
import singer
from singer import utils
from typing import Dict, Optional, List
from tap_linkedin_ads.client import LinkedinClient
from tap_linkedin_ads.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    "start_date",
    "user_agent",
    "client_id",
    "client_secret",
    "refresh_token",
    "accounts",
]


@singer.utils.handle_top_exception(LOGGER)
def main():
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    state = parsed_args.state or {}
    tap(client_id, client_secret, refresh_token, user_agent, state)


def tap(config: Dict, state: Optional[Dict]):
    print(config)
    client_id = config["client_id"]
    client_secret = config["client_secret"]
    refresh_token = config["refresh_token"]
    user_agent = config["user_agent"]

    # if no accounts have been enabled
    # then some requests will fail
    # - therefore we abort here
    accounts: List[str] = config.get("accounts", [])
    if not accounts:
        return

    with LinkedinClient(client_id, client_secret, refresh_token, user_agent) as client:
        sync(
            client=client,
            config=config,
            state=state,
        )


if __name__ == "__main__":
    import json

    with open("config.json") as fp:
        config = json.load(fp)
    with open("state.json") as fp:
        state = json.load(fp)

    tap(config, state)
