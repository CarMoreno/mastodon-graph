from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import current_thread

import polars as pl
from collections.abc import Iterable
from entities import User, Author
from config import mastodon
from logger import logger


def fetch_author_by_chunk(
    start_offset: int, max_num_to_fetch: int
) -> tuple[int, list[User]]:
    try:
        limit = 40
        offset = start_offset
        current_results = 0
        authors_chunk = []
        thread_id = current_thread().ident
        log_prefix = f"[Author | ID: {thread_id}]"
        logger.info(
            f"{log_prefix} Stating. Objective: {max_num_to_fetch} authors from offset {start_offset}."
        )
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
                )
                authors_chunk.append(author)
                current_results += 1
            offset += limit
        return thread_id, authors_chunk
    except Exception as ex:
        logger.error(f"Exception while fetching authors: {ex}")


def get_author_parallel(
    num_threads: int = 5, results_per_thread: int = 1000
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
                fetch_author_by_chunk, start_offset, results_per_thread
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


if __name__ == "__main__":
    NUM_THREADS = 5
    RESULTS_BY_THREAD = 1000

    logger.info(
        f"Starting parallel execution using {NUM_THREADS} threads (each one will look for {RESULTS_BY_THREAD} results)..."
    )

    # orchestrator function gets the full list of authoers
    total_authors = get_author_parallel(
        num_threads=NUM_THREADS, results_per_thread=RESULTS_BY_THREAD
    )

    if total_authors:
        author_df = pl.DataFrame(total_authors)

        logger.info("--- Results DF ---")
        logger.info(author_df)
        logger.info(f"Totals: {len(author_df)}")
    else:
        logger.info("There are not authors to fetch!")
