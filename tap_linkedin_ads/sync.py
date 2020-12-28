import urllib.parse
import datetime
from datetime import timedelta
import singer
from singer import (
    metrics,
    utils,
)
from singer.utils import strptime_to_utc, strftime
from tap_linkedin_ads.transform import transform_json

LOGGER = singer.get_logger()
DATE_WINDOW_SIZE = 30 # days


def write_record(stream_name, record, time_extracted):
    try:
        singer.write_record(stream_name, record, time_extracted=time_extracted)
    except OSError as err:
        LOGGER.info("OS Error writing record for: {}".format(stream_name))
        LOGGER.info("record: {}".format(record))
        raise err


def get_bookmark(state, stream, default):
    if (state is None) or ("bookmarks" not in state):
        return default
    return state.get("bookmarks", {}).get(stream, default)


def write_bookmark(state, stream, value):
    if "bookmarks" not in state:
        state["bookmarks"] = {}
    state["bookmarks"][stream] = value
    LOGGER.info("Write state for stream: {}, value: {}".format(stream, value))
    singer.write_state(state)


def unixseconds_to_datetime(ms: str) -> datetime.datetime:
    try:
        milliseconds = int(ms)
        return datetime.datetime.fromtimestamp(
            milliseconds / 1000.0, datetime.timezone.utc
        )
    except ValueError:
        return strptime_to_utc(ms)


def process_records(
    stream_name,
    records,
    time_extracted,
    bookmark_field=None,
    max_bookmark_value=None,
    last_datetime=None,
    parent=None,
    parent_id=None,
):

    with metrics.record_counter(stream_name) as counter:
        for record in records:
            # If child object, add parent_id to record
            if parent_id and parent:
                record[parent + "_id"] = parent_id

                # Reset max_bookmark_value to new value if higher
            if bookmark_field and (bookmark_field in record):
                bookmark_value = unixseconds_to_datetime(record[bookmark_field])

                if max_bookmark_value is None or bookmark_value > max_bookmark_value:
                    max_bookmark_value = bookmark_value

                # # Keep only records whose bookmark is after the last_datetime
                if bookmark_value >= strptime_to_utc(last_datetime):
                    write_record(
                        stream_name,
                        record,
                        time_extracted=time_extracted,
                    )
                    counter.increment()
            else:
                write_record(stream_name, record, time_extracted=time_extracted)
                counter.increment()

        return max_bookmark_value, counter.value


# Sync a specific parent or child endpoint.
def sync_endpoint(
    client,  # pylint: disable=too-many-branches
    state,
    start_date,
    stream_name,
    path,
    endpoint_config,
    data_key,
    static_params,
    bookmark_query_field=None,
    bookmark_field=None,
    id_fields=None,
    parent=None,
    parent_id=None,
):

    # Get the latest bookmark for the stream and set the last_datetime
    last_datetime = get_bookmark(state, stream_name, start_date)
    max_bookmark_value = strptime_to_utc(last_datetime)
    LOGGER.info(
        "{}: bookmark last_datetime = {}".format(stream_name, max_bookmark_value)
    )

    # Initialize child_max_bookmarks
    child_max_bookmarks = {}
    children = endpoint_config.get("children")
    if children:
        for child_stream_name, child_endpoint_config in children.items():
            child_bookmark_field = child_endpoint_config.get("bookmark_field")
            if child_bookmark_field:
                child_last_datetime = get_bookmark(state, stream_name, start_date)
                child_max_bookmarks[child_stream_name] = child_last_datetime

    # Pagination reference:
    # https://docs.microsoft.com/en-us/linkedin/shared/api-guide/concepts/pagination?context=linkedin/marketing/context
    # Each page has a "start" (offset value) and a "count" (batch size, number of records)
    # Increase the "start" by the "count" for each batch.
    # Continue until the "start" exceeds the total_records.
    start = 0  # Starting offset value for each batch API call
    count = 100  # Batch size; Number of records per API call
    total_records = 0
    page = 1
    params = {
        "start": start,
        "count": count,
        **static_params,  # adds in endpoint specific, sort, filter params
    }
    if bookmark_query_field:
        params[bookmark_query_field] = last_datetime

    querystring = "&".join(["%s=%s" % (key, value) for (key, value) in params.items()])
    next_url = "https://api.linkedin.com/v2/{}?{}".format(path, querystring)

    while next_url:
        LOGGER.info("URL for {}: {}".format(stream_name, next_url))

        # Get data, API request
        data = client.get(url=next_url, endpoint=stream_name)
        # time_extracted: datetime when the data was extracted from the API
        time_extracted = utils.now()
        # LOGGER.info('stream_name = , data = {}'.format(stream_name, data))  # TESTING, comment out

        # Transform data with transform_json from transform.py
        #  This function converts unix datetimes, de-nests audit fields,
        #  tranforms URNs to IDs, tranforms/abstracts variably named fields,
        #  converts camelCase to snake_case for fieldname keys.
        # For the Linkedin Ads API, 'elements' is always the root data_key for records.
        # The data_key identifies the collection of records below the <root> element
        transformed_data = []  # initialize the record list
        if data_key in data:
            transformed_data = transform_json(data, stream_name)[data_key]
        # LOGGER.info('stream_name = , transformed_data = {}'.format(stream_name, transformed_data))  # TESTING, comment out
        if not transformed_data or transformed_data is None:
            LOGGER.info("No transformed_data")
            # LOGGER.info('data_key = {}, data = {}'.format(data_key, data))
            break  # No data results

        # Process records and get the max_bookmark_value and record_count for the set of records
        max_bookmark_value, record_count = process_records(
            stream_name=stream_name,
            records=transformed_data,
            time_extracted=time_extracted,
            bookmark_field=bookmark_field,
            max_bookmark_value=max_bookmark_value,
            last_datetime=last_datetime,
            parent=parent,
            parent_id=parent_id,
        )
        LOGGER.info("{}, records processed: {}".format(stream_name, record_count))
        total_records = total_records + record_count

        # Loop thru parent batch records for each children objects (if should stream)
        if children:
            for child_stream_name, child_endpoint_config in children.items():

                # For each parent record
                for record in transformed_data:
                    i = 0
                    # Set parent_id
                    for id_field in id_fields:
                        if i == 0:
                            parent_id_field = id_field
                        if id_field == "id":
                            parent_id_field = id_field
                        i = i + 1
                    parent_id = record.get(parent_id_field)
                    # Add children filter params based on parent IDs
                    if stream_name == "accounts":
                        account = "urn:li:sponsoredAccount:{}".format(parent_id)
                        owner_id = record.get("reference_organization_id", None)
                        owner = "urn:li:organization:{}".format(owner_id)
                        if child_stream_name == "video_ads" and owner_id is not None:
                            child_endpoint_config["params"]["account"] = account
                            child_endpoint_config["params"]["owner"] = owner
                    elif stream_name == "campaigns":
                        campaign = "urn:li:sponsoredCampaign:{}".format(parent_id)
                        if child_stream_name == "creatives":
                            child_endpoint_config["params"][
                                "search.campaign.values[0]"
                            ] = campaign
                        elif child_stream_name in (
                            "ad_analytics_by_campaign",
                            "ad_analytics_by_creative",
                        ):
                            child_endpoint_config["params"]["campaigns[0]"] = campaign

                    LOGGER.info(
                        "Syncing: {}, parent_stream: {}, parent_id: {}".format(
                            child_stream_name, stream_name, parent_id
                        )
                    )
                    child_path = child_endpoint_config.get("path")
                    if child_stream_name in (
                            "ad_analytics_by_campaign",
                            "ad_analytics_by_creative",
                        ):
                        child_total_records, child_batch_bookmark_value = sync_ad_analytics(
                            client=client,
                            state=state,
                            start_date=start_date,
                            stream_name=child_stream_name,
                            path=child_path,
                            endpoint_config=child_endpoint_config,
                            data_key=child_endpoint_config.get("data_key", "elements"),
                            static_params=child_endpoint_config.get("params", {}),
                            bookmark_query_field=child_endpoint_config.get(
                                "bookmark_query_field"
                            ),
                            bookmark_field=child_endpoint_config.get("bookmark_field"),
                            id_fields=child_endpoint_config.get("id_fields"),
                            parent=child_endpoint_config.get("parent"),
                            parent_id=parent_id,
                        )
                    else:
                        child_total_records, child_batch_bookmark_value = sync_endpoint(
                            client=client,
                            state=state,
                            start_date=start_date,
                            stream_name=child_stream_name,
                            path=child_path,
                            endpoint_config=child_endpoint_config,
                            data_key=child_endpoint_config.get("data_key", "elements"),
                            static_params=child_endpoint_config.get("params", {}),
                            bookmark_query_field=child_endpoint_config.get(
                                "bookmark_query_field"
                            ),
                            bookmark_field=child_endpoint_config.get("bookmark_field"),
                            id_fields=child_endpoint_config.get("id_fields"),
                            parent=child_endpoint_config.get("parent"),
                            parent_id=parent_id,
                        )

                    child_batch_bookmark_dttm = child_batch_bookmark_value
                    child_max_bookmark = child_max_bookmarks.get(child_stream_name)
                    child_max_bookmark_dttm = strptime_to_utc(child_max_bookmark)
                    if child_batch_bookmark_dttm > child_max_bookmark_dttm:
                        child_max_bookmarks[child_stream_name] = strftime(
                            child_batch_bookmark_dttm
                        )

                    LOGGER.info(
                        "Synced: {}, parent_id: {}, total_records: {}".format(
                            child_stream_name, parent_id, child_total_records
                        )
                    )

        # Pagination: Get next_url
        next_url = None
        links = data.get("paging", {}).get("links", [])
        for link in links:
            rel = link.get("rel")
            if rel == "next":
                href = link.get("href")
                if href:
                    next_url = "https://api.linkedin.com{}".format(
                        urllib.parse.unquote(href)
                    )

        LOGGER.info(
            "{}: Synced page {}, this page: {}. Total records processed: {}".format(
                stream_name, page, record_count, total_records
            )
        )
        page = page + 1

    # Write child bookmarks
    for key, val in list(child_max_bookmarks.items()):
        write_bookmark(state, key, val)

    return total_records, max_bookmark_value


# Currently syncing sets the stream currently being delivered in the state.
# If the integration is interrupted, this state property is used to identify
#  the starting point to continue from.
# Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
def update_currently_syncing(state, stream_name):
    if (stream_name is None) and ("currently_syncing" in state):
        del state["currently_syncing"]
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


def sync(client, config, state):
    if "start_date" in config:
        start_date = config["start_date"]

    # Get datetimes for endpoint parameters
    now = utils.now()
    # delta = 7 days to account for delays in ads data
    delta = 7
    analytics_campaign_dt_str = get_bookmark(
        state, "ad_analytics_by_campaign", start_date
    )
    analytics_campaign_dt = strptime_to_utc(analytics_campaign_dt_str) - timedelta(
        days=delta
    )
    analytics_creative_dt_str = get_bookmark(
        state, "ad_analytics_by_creative", start_date
    )
    analytics_creative_dt = strptime_to_utc(analytics_creative_dt_str) - timedelta(
        days=delta
    )

    # last_stream = Previous currently synced stream, if the load was interrupted
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info("last/currently syncing stream: {}".format(last_stream))

    # endpoints: API URL endpoints to be called
    # properties:
    #   <root node>: Plural stream name for the endpoint
    #   path: API endpoint relative path, when added to the base URL, creates the full path
    #   account_filter: Method for Account filtering. Each uses a different query pattern/parameter:
    #        search_id_values_param, search_account_values_param, accounts_param
    #   params: Query, sort, and other endpoint specific parameters
    #   data_key: JSON element containing the records for the endpoint
    #   bookmark_query_field: Typically a date-time field used for filtering the query
    #   bookmark_field: Replication key field, typically a date-time, used for filtering the results
    #        and setting the state
    #   store_ids: Used for parents to create an id_bag collection of ids for children endpoints
    #   id_fields: Primary key (and other IDs) from the Parent stored when store_ids is true.
    #   children: A collection of child endpoints (where the endpoint path includes the parent id)
    #   parent: On each of the children, the singular stream name for parent element
    #       NOT NEEDED FOR THIS INTEGRATION (The Children all include a reference to the Parent)
    endpoints = {
        "accounts": {
            "path": "adAccountsV2",
            "account_filter": "search_id_values_param",
            "params": {"q": "search", "sort.field": "ID", "sort.order": "ASCENDING"},
            "data_key": "elements",
            "bookmark_field": "last_modified_time",
            "id_fields": ["id", "reference_organization_id"],
            "children": {
                "video_ads": {
                    "path": "adDirectSponsoredContents",
                    "account_filter": None,
                    "params": {"q": "account"},
                    "data_key": "elements",
                    "bookmark_field": "last_modified_time",
                    "id_fields": ["content_reference"],
                }
            },
        },
        "account_users": {
            "path": "adAccountUsersV2",
            "account_filter": "accounts_param",
            "params": {"q": "accounts"},
            "data_key": "elements",
            "bookmark_field": "last_modified_time",
            "id_fields": ["account_id", "user_person_id"],
        },
        "campaign_groups": {
            "path": "adCampaignGroupsV2",
            "account_filter": "search_account_values_param",
            "params": {"q": "search", "sort.field": "ID", "sort.order": "ASCENDING"},
            "data_key": "elements",
            "bookmark_field": "last_modified_time",
            "id_fields": ["id"],
        },
        "campaigns": {
            "path": "adCampaignsV2",
            "account_filter": "search_account_values_param",
            "params": {"q": "search", "sort.field": "ID", "sort.order": "ASCENDING"},
            "data_key": "elements",
            "bookmark_field": "last_modified_time",
            "id_fields": ["id"],
            "children": {
                "ad_analytics_by_campaign": {
                    "path": "adAnalyticsV2",
                    "account_filter": "accounts_param",
                    "params": {
                        "q": "analytics",
                        "pivot": "CAMPAIGN",
                        "timeGranularity": "DAILY",
                        "dateRange.start.day": analytics_campaign_dt.day,
                        "dateRange.start.month": analytics_campaign_dt.month,
                        "dateRange.start.year": analytics_campaign_dt.year,
                        "dateRange.end.day": now.day,
                        "dateRange.end.month": now.month,
                        "dateRange.end.year": now.year,
                        "fields": "pivotValues,impressions,clicks,likes,dateRange,videoStarts,videoViews,costInLocalCurrency,reactions,sends,shares,totalEngagements,pivot,pivotValue",
                        "count": 10000
                    },
                    "data_key": "elements",
                    "bookmark_field": "end_at",
                    "id_fields": ["creative_id", "start_at"],
                },
                "creatives": {
                    "path": "adCreativesV2",
                    "account_filter": None,
                    "params": {
                        "q": "search",
                        "search.campaign.values[0]": "urn:li:sponsoredCampaign:{}",
                        "sort.field": "ID",
                        "sort.order": "ASCENDING",
                    },
                    "data_key": "elements",
                    "bookmark_field": "last_modified_time",
                    "id_fields": ["id"],
                },
            },
        },
    }

    # For each endpoint (above), determine if the stream should be streamed
    #   (based on the catalog and last_stream), then sync those streams.
    for stream_name, endpoint_config in endpoints.items():

        # Add appropriate account_filter query parameters based on account_filter type
        account_filter = endpoint_config.get("account_filter", None)
        if "accounts" in config and account_filter is not None:
            account_list = config["accounts"]
            for idx, account in enumerate(account_list):
                if account_filter == "search_id_values_param":
                    endpoint_config["params"]["search.id.values[{}]".format(idx)] = int(
                        account
                    )
                elif account_filter == "search_account_values_param":
                    endpoint_config["params"][
                        "search.account.values[{}]".format(idx)
                    ] = "urn:li:sponsoredAccount:{}".format(account)
                elif account_filter == "accounts_param":
                    endpoint_config["params"][
                        "accounts[{}]".format(idx)
                    ] = "urn:li:sponsoredAccount:{}".format(account)

        LOGGER.info("START Syncing: {}".format(stream_name))
        update_currently_syncing(state, stream_name)
        path = endpoint_config.get("path")
        bookmark_field = endpoint_config.get("bookmark_field")
        total_records, max_bookmark_value = sync_endpoint(
            client=client,
            state=state,
            start_date=start_date,
            stream_name=stream_name,
            path=path,
            endpoint_config=endpoint_config,
            data_key=endpoint_config.get("data_key", "elements"),
            static_params=endpoint_config.get("params", {}),
            bookmark_query_field=endpoint_config.get("bookmark_query_field"),
            bookmark_field=bookmark_field,
            id_fields=endpoint_config.get("id_fields"),
        )

        # Write parent bookmarks
        if bookmark_field:
            write_bookmark(state, stream_name, strftime(max_bookmark_value))

        update_currently_syncing(state, None)
        LOGGER.info("Synced: {}, total_records: {}".format(stream_name, total_records))
        LOGGER.info("FINISHED Syncing: {}".format(stream_name))

def split_into_chunks(fields, chunk_length):
    return (fields[x:x+chunk_length] for x in range(0, len(fields), chunk_length))

def shift_sync_window(params, today):
    current_end = datetime.date(
        year=params['dateRange.end.year'],
        month=params['dateRange.end.month'],
        day=params['dateRange.end.day'],
    )

    new_end = current_end + timedelta(days=DATE_WINDOW_SIZE)

    if new_end > today:
        new_end = today

    new_params = {**params,
                  'dateRange.start.day': current_end.day,
                  'dateRange.start.month': current_end.month,
                  'dateRange.start.year': current_end.year,

                  'dateRange.end.day': new_end.day,
                  'dateRange.end.month': new_end.month,
                  'dateRange.end.year': new_end.year,}
    return current_end, new_end, new_params
    
def merge_responses(data):
    full_records = dict()
    for page in data:
        for element in page:
            temp_start = element['dateRange']['start']
            temp_pivotValue = element['pivotValue']
            string_start = '{}-{}-{}'.format(temp_start['year'], temp_start['month'], temp_start['day'])
            primary_key = (temp_pivotValue, string_start)
            if primary_key in full_records:
                full_records[primary_key].update(element)
            else:
                full_records[primary_key] = element
    return full_records

def sync_ad_analytics(client, state, start_date, stream_name, path, endpoint_config, data_key, static_params,
                      bookmark_query_field=None, bookmark_field=None, id_fields=None, parent=None, parent_id=None):
    # pylint: disable=too-many-branches,too-many-statements,unused-argument


    last_timestamp = get_bookmark(state, stream_name, start_date)
    last_datetime_dt = strptime_to_utc(last_timestamp)

    window_start_date = last_datetime_dt.date()
    window_end_date = window_start_date + timedelta(days=DATE_WINDOW_SIZE)
    today = datetime.date.today()

    if window_end_date > today:
        window_end_date = today

    # Override the default start and end dates
    static_params = {**static_params,
                     'dateRange.start.day': window_start_date.day,
                     'dateRange.start.month': window_start_date.month,
                     'dateRange.start.year': window_start_date.year,

                     'dateRange.end.day': window_end_date.day,
                     'dateRange.end.month': window_end_date.month,
                     'dateRange.end.year': window_end_date.year,}

    # When testing the API, if the fields in `field` all return `0` then
    # the API returns its empty response.

    # However, the API distinguishes between a day with non-null values
    # (even if this means the values are all `0`) and a day with null
    # values. We found that requesting these fields give you the days with
    # non-null values
    required_fields = set(['dateRange', 'pivot', 'pivotValue'])
    # LinkedIn has a max of 20 fields per request. We cap the chunks at 17
    # to make sure there's always room for us to append `dateRange`,
    # `pivot`, and `pivotValue`
    MAX_CHUNK_LENGTH = 20 - len(required_fields) # 17

    # given fields:
    valid_selected_fields = list(set(static_params["fields"].split(",")) - required_fields)

    # all requests need to have required fields, but no more than 20 fields can be requested at once
    # We have to append these fields in order to ensure we get them back
    # so that we can create the composite primary key for the record and
    # to merge the multiple responses based on this primary key
    chunks = []
    for field_chunk in split_into_chunks(valid_selected_fields, MAX_CHUNK_LENGTH):
        chunks.append([*field_chunk, *required_fields])


    total_records = 0
    while window_end_date <= today:
        responses = []
        for chunk in chunks:
            static_params['fields'] = ','.join(chunk)
            params = {"start": 0,
                      "count": static_params.get('count', 100),
                      **static_params}
            query_string = '&'.join(['%s=%s' % (key, value) for (key, value) in params.items()])
            LOGGER.info('Syncing %s from %s to %s', parent_id, window_start_date, window_end_date)
            for page in sync_analytics_endpoint(client, stream_name, endpoint_config.get('path'), query_string):
                data = page.get(data_key)
                if data:
                    responses.append(data)
        raw_records = merge_responses(responses)
        time_extracted = utils.now()

        # While we broke the ad_analytics streams out from
        # `sync_endpoint()`, we want to process them the same. And
        # transform_json() expects a dictionary with a key equal to
        # `data_key` and its value is the response from the API

        # Note that `transform_json()` returns the same structure we pass
        # in. `sync_endpoint()` grabs `data_key` from the return value, so
        # we mirror that here
        transformed_data = transform_json({data_key: list(raw_records.values())},
                                          stream_name)[data_key]
        if not transformed_data:
            LOGGER.info('No transformed_data')
        else:
            max_bookmark_value, record_count = process_records(
                stream_name=stream_name,
                records=transformed_data,
                time_extracted=time_extracted,
                bookmark_field=bookmark_field,
                max_bookmark_value=last_datetime_dt,
                last_datetime=start_date,
                parent=parent,
                parent_id=parent_id,
            )
            LOGGER.info('%s, records processed: %s', stream_name, record_count)
            LOGGER.info('%s: max_bookmark: %s', stream_name, max_bookmark_value)
            total_records += record_count

        window_start_date, window_end_date, static_params = shift_sync_window(static_params, today)

        if window_start_date == window_end_date:
            break

    return total_records, last_datetime_dt

def sync_analytics_endpoint(client, stream_name, path, query_string):
    page = 1
    next_url = 'https://api.linkedin.com/v2/{}?{}'.format(path, query_string)

    while next_url:
        LOGGER.info('URL for %s: %s', stream_name, next_url)

        data = client.get(url=next_url, endpoint=stream_name)
        yield data
        next_url = get_next_url(data)

        LOGGER.info('%s: Synced page %s', stream_name, page)
        page = page + 1


def get_next_url(data):
    next_url = None
    links = data.get('paging', {}).get('links', [])
    for link in links:
        rel = link.get('rel')
        if rel == 'next':
            href = link.get('href')
            if href:
                next_url = 'https://api.linkedin.com{}'.format(urllib.parse.unquote(href))
    return next_url