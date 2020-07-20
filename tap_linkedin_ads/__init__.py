#!/usr/bin/env python3

import singer
from singer import utils
from tap_linkedin_ads.client import LinkedinClient
from tap_linkedin_ads.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["start_date", "user_agent", "access_token", "accounts"]


@singer.utils.handle_top_exception(LOGGER)
def main():

    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    with LinkedinClient(
        access_token=parsed_args.config["access_token"],
        user_agent=parsed_args.config["user_agent"],
    ) as client:
        state = {}
        if parsed_args.state:
            state = parsed_args.state
        sync(
            client=client, config=parsed_args.config, state=state,
        )


if __name__ == "__main__":
    main()
