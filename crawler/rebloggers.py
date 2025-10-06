from models.entities import Reblogger
from config.mastodon import mastodon
from utils.logger import logger


def get_rebloggers_by_chunks(author_info: list[tuple]) -> list[Reblogger]:
    all_rebloggers = []
    for status_id, author_id in author_info:
        logger.info("Getting rebloggers from the first page.")
        page = 1
        current_page = mastodon.status_reblogged_by(status_id)
        logger.info(f"There are {len(current_page)} rebloggers.")
        rebloggers = list(current_page)
        while current_page:
            page += 1
            logger.info(f"Searching for the next page: # {page}")
            next_page = mastodon.fetch_next(current_page)
            if next_page:
                logger.info(f"Found next page. There are {len(next_page)} rebloggers")
                rebloggers.extend(next_page)
                current_page = next_page
            else:
                logger.info("All rebloggers were found.")
                break

        all_rebloggers.extend(
            Reblogger(
                id=r["id"],
                username=r["username"],
                acct=r["acct"],
                author_id=str(author_id),
                instance="carlos.social",
            )
            for r in rebloggers
        )

    return all_rebloggers


if __name__ == "__main__":
    author_info = [(115310498566920494, 565921)]  # status_id, author_id
    rebloggers = get_rebloggers_by_chunks(author_info)
    print(rebloggers)
    print(len(rebloggers))
