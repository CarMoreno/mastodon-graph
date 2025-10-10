import time
import os
import polars as pl
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from mastodon import MastodonNotFoundError, MastodonAPIError

from models.entities import Reblogger
from config.mastodon import mastodon
from utils.logger import logger

TEMP_OUTPUT_DIR = "../data/rebloggers_temp"
FINAL_OUTPUT_FILE = "../data/rebloggers.csv"


def process_and_save_rebloggers(author_info: tuple) -> str | None:
    """
    Get the rebloggers for each toot and save the result in a .parquet file
    It returns the path to the .parquet file if all is OK, None otherwise.
    """
    thread_id = threading.get_ident()
    thread_name = threading.current_thread().name
    toot_id, author_id, instance = author_info

    # Path to the parquet file for this task
    output_path = os.path.join(TEMP_OUTPUT_DIR, f"{toot_id}.parquet")

    log_prefix = f"[{thread_name} | ID: {thread_id} | Toot_ID: {toot_id}]"
    logger.info(f"{log_prefix} Starting task.")

    mastodon.api_base_url = f"https://{instance}"

    try:
        current_page = mastodon.status_reblogged_by(toot_id)
        raw_rebloggers = list(current_page)

        while current_page:
            time.sleep(1)  # Mantener la pausa es buena idea
            next_page = mastodon.fetch_next(current_page)
            if next_page:
                raw_rebloggers.extend(next_page)
                current_page = next_page
            else:
                break

        if not raw_rebloggers:
            logger.info(f"{log_prefix} There are not rebloggers.")
            return None

        # Create the Rebloggers objects and the dataframe
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
        df_chunk = pl.DataFrame(reblogger_objects)

        # Save the result of THIS TASK in its own file.
        df_chunk.write_parquet(output_path)
        logger.info(
            f"{log_prefix} Success. {len(df_chunk)} rebloggers save in {output_path}"
        )

        return output_path

    except (MastodonNotFoundError, MastodonAPIError) as e:
        logger.error(f"{log_prefix} API error for {instance}: {e.args}")
        return None
    except Exception as e:
        logger.error(f"{log_prefix} Critical error for {instance}: {e.args}")
        return None


def run_parallel_processing(author_info_filtered: pl.DataFrame, num_threads: int = 10):
    """
    Orchestrate the threads to process only pending tasks.
    """
    with ThreadPoolExecutor(
        max_workers=num_threads, thread_name_prefix="RebloggerFetcher"
    ) as executor:
        futures = {
            executor.submit(process_and_save_rebloggers, row): row
            for row in author_info_filtered.iter_rows()
        }

        for future in as_completed(futures):
            try:
                result_path = future.result()
                if result_path:
                    logger.info(f"New file saved in {result_path}")
            except Exception as e:
                logger.error(f"A task in the pool critically fails {e}")


def merge_temp_files() -> None | pl.DataFrame:
    """
    Read all the .parquet files in the temporal folder and save them as a unique CSV file
    """
    logger.info(f"Uniendo archivos temporales de {TEMP_OUTPUT_DIR}...")

    try:
        df_all_rebloggers = pl.read_parquet(os.path.join(TEMP_OUTPUT_DIR, "*.parquet"))

        if len(df_all_rebloggers) > 0:
            df_all_rebloggers.write_csv(FINAL_OUTPUT_FILE)
            logger.info(
                f"Success. {len(df_all_rebloggers)} rebloggers saved in {FINAL_OUTPUT_FILE}"
            )
            return df_all_rebloggers
        else:
            logger.info(f"There are not temporal files in {TEMP_OUTPUT_DIR}")
            return None
    except Exception as e:
        logger.error(f"Critical errors, temporal files could not be joined: {e}")
        return None


if __name__ == "__main__":
    # 1. Create the temporal folder just in case it does not exist
    os.makedirs(TEMP_OUTPUT_DIR, exist_ok=True)

    # 2. Check the previous job
    logger.info("Revisando tareas ya completadas...")
    completed_toot_ids = {
        int(f.split(".")[0])
        for f in os.listdir(TEMP_OUTPUT_DIR)
        if f.endswith(".parquet")
    }
    logger.info(f"Se encontraron {len(completed_toot_ids)} tareas ya completadas.")

    # 3. Load and filter the pending tasks
    author_info_full = (
        pl.read_csv("../data/authors.csv")
        .select("toot_id", pl.col("id").alias("author_id"), "instance")
        .sort(by="author_id")
    )

    author_info_pending = author_info_full.filter(
        ~pl.col("toot_id").is_in(completed_toot_ids)
    ).limit(100)

    logger.info(
        f"Total de tareas a procesar: {len(author_info_full)}. Tareas pendientes: {len(author_info_pending)}"
    )

    # 4. Execute the parallel processing for pending tasks only
    if len(author_info_pending) > 0:
        start = time.time()
        run_parallel_processing(
            author_info_filtered=author_info_pending, num_threads=10
        )
        logger.info(
            "Parallel processing finished in {:.2f} seconds.".format(
                time.time() - start
            )
        )
    else:
        logger.info("There are no new tasks to be processed.")

    # 5. Join all the .parquets files in one CSV final file.
    merge_temp_files()
