import time

import polars as pl
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import current_thread

from mastodon import MastodonNotFoundError, MastodonAPIError

from models.entities import Reblogger
from config.mastodon import mastodon
from utils.logger import logger


def get_reblogger_parallel(
    author_info: pl.DataFrame,
    num_threads: int = 7,
) -> list[Reblogger]:
    with ThreadPoolExecutor(num_threads) as executor:
        futures = []
        rebloggers = []
        for info in author_info.iter_rows():
            futures.append(executor.submit(get_rebloggers_by_chunks, info))

        for future in as_completed(futures):
            thread_id, reblogger = future.result()
            logger.info(f"Thread ID: {thread_id} Finished")
            rebloggers.extend(reblogger)
    return rebloggers


def get_rebloggers_by_chunks(author_info: tuple) -> tuple[int, list[Reblogger]]:
    thread_id = current_thread().ident
    status_id, author_id, instance = author_info
    log_prefix = f"[Reblogger | ID: {thread_id} | Status ID: {status_id}]"
    logger.info(f"{log_prefix} Getting rebloggers from the first page.")
    page = 1
    try:
        current_page = mastodon.status_reblogged_by(status_id)
        logger.info(f"{log_prefix} There are {len(current_page)} rebloggers.")
        raw_rebloggers = list(current_page)
        while current_page:
            page += 1
            time.sleep(3)
            logger.info(f"{log_prefix} Searching for the next page: # {page}")
            next_page = mastodon.fetch_next(current_page)
            if next_page:
                logger.info(
                    f"{log_prefix} Found next page. There are {len(next_page)} rebloggers"
                )
                raw_rebloggers.extend(next_page)
                current_page = next_page
            else:
                logger.info(f"{log_prefix} All rebloggers were found.")
                break

        reblogger_objects = [
            Reblogger(
                id=r["id"],
                username=r["username"],
                acct=r["acct"],
                author_id=str(author_id),
                instance=instance,
            )
            for r in raw_rebloggers
        ]
        return thread_id, reblogger_objects
    except MastodonNotFoundError as e:
        logger.error(f"{log_prefix} Endpoint for {instance}, does not exist: {e.args}")
        return thread_id, []
    except MastodonAPIError as e:
        logger.error(f"{log_prefix} Mastodon API error for {instance}: {e.args}")
        return thread_id, []


if __name__ == "__main__":
    # author_info = [(115310498566920494, 565921, "mastodon.social")]  # status_id, author_id
    author_info = pl.read_csv("../data/authors_03102025_173414.csv").select(
        "status_id", pl.col("id").alias("author_id"), "instance"
    )  # .limit(10)
    rebloggers = get_reblogger_parallel(author_info=author_info)
    df_rebloggers = pl.DataFrame(rebloggers)
    df_rebloggers.write_csv("../data/rebloggers.csv")
    logger.info(rebloggers)
    logger.info(len(rebloggers))
