from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from threading import current_thread
import polars as pl
import itertools as it
from collections.abc import Iterable
from mastodon import MastodonNotFoundError

from entities import User, Author
from config import mastodon
from mastodon.errors import MastodonAPIError
from logger import logger


def fetch_author_by_chunk(
    start_offset: int, max_num_to_fetch: int, instance: str
) -> tuple[int, list[User]]:
    thread_id = current_thread().ident
    try:
        limit = 40
        offset = start_offset
        current_results = 0
        authors_chunk = []
        log_prefix = f"[Author | ID: {thread_id}]"
        logger.info(
            f"{log_prefix} Stating. Objective: {max_num_to_fetch} authors from offset {start_offset}."
        )
        mastodon.api_base_url = f"https://{instance}"
        while current_results < max_num_to_fetch:
            statuses = mastodon.trending_statuses(limit=limit, offset=offset)
            if not statuses:
                break
            for status in statuses:
                account = status["account"]
                author = Author(
                    id=account["id"],
                    username=account["username"],
                    acct=account["acct"],
                    status_id=status["id"],
                    instance=instance,
                )
                authors_chunk.append(author.model_dump())
                current_results += 1
            offset += limit
        return thread_id, authors_chunk
    except MastodonNotFoundError as e:
        logger.info(f"Endpoint for {instance}, does not exist: {e.args}")
        return thread_id, []
    except MastodonAPIError as e:
        logger.error(f"Mastodon API error for {instance}: {e.args}")


def get_author_parallel(
    num_threads: int = 5,
    results_per_thread: int = 1000,
    instance: str = "mastodon.social",
) -> Iterable[Author]:
    """
    Manage multiple parallel threads to get authors by chunks
    """
    all_authors = []

    with ThreadPoolExecutor(
        max_workers=num_threads, thread_name_prefix="AuthorFetcher"
    ) as executor:
        futures = []
        for i in range(num_threads):
            start_offset = i * results_per_thread
            future = executor.submit(
                fetch_author_by_chunk, start_offset, results_per_thread, instance
            )
            futures.append(future)

        for future in as_completed(futures):
            try:
                thread_id, result_chunk = future.result()
                logger.info(
                    f"Thread {thread_id} finished, {len(result_chunk)} results: {result_chunk[:3]}"
                )
                all_authors.extend(result_chunk)
            except Exception as e:
                logger.error(f"Una tarea falló con el error: {e}")

    return all_authors


def collect_authors(
    instances: list[str], n_threads: int, results_by_thread: int
) -> pl.LazyFrame:
    logger.info(
        f"Starting parallel execution using {n_threads} threads (each one will look for {results_by_thread} results)..."
    )
    # orchestrator function gets the full list of authors
    authors = iter([])
    for instance in instances:
        authors_by_instance = get_author_parallel(
            num_threads=n_threads,
            results_per_thread=results_by_thread,
            instance=instance,
        )
        if authors_by_instance:
            authors = it.chain(authors, authors_by_instance)
            logger.info(f"Instance {instance} Totals: {len(list(authors_by_instance))}")
        else:
            logger.info(f"There are not authors to fetch! for {instance}")

    author_df = pl.LazyFrame(authors)
    return author_df


def main() -> None:
    NUM_THREADS = 5
    RESULTS_BY_THREAD = 1000
    now = datetime.now()
    instances = [
        "mastodon.social",
        "gts.turtle.garden",
        "mstdn.social",
        "mastodon.world",
        "mas.to",
        "techhub.social",
        "universeodon.com",
        "mastodonapp.uk",
        "c.im",
        "loforo.com",
        "fosstodon.org",
        "mstdn.party",
        "aethy.com",
        "mastodon.nl",
        "piaille.fr",
        "kolektiva.social",
        "mastodon.art",
        "mamot.fr",
    ]
    authors_df = collect_authors(instances, NUM_THREADS, RESULTS_BY_THREAD).collect()
    authors_df.write_csv(f"authors_{now.strftime('%d%m%Y_%H%M%S')}.csv")
    logger.info("Total number of authors: %d" % len(authors_df))


if __name__ == "__main__":
    main()
