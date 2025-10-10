from datetime import datetime
from crawler.author import collect_authors
from utils.logger import logger


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
        "mastodon.top",
        "aethy.com",
        "mastodon.nl",
        "piaille.fr",
        "kolektiva.social",
        "mastodon.art",
        "mamot.fr",
    ]
    authors_df = collect_authors(instances, NUM_THREADS, RESULTS_BY_THREAD).collect()
    authors_df.write_csv(f"data/authors_{now.strftime('%d%m%Y_%H%M%S')}.csv")
    logger.info("Total number of authors: %d" % len(authors_df))


if __name__ == "__main__":
    import polars as pl

    authors = pl.concat(
        [pl.read_csv("data/authors_03*.csv"), pl.read_csv("data/authors_04*.csv")]
    ).unique(maintain_order=True)
    print(len(authors))
    authors.write_csv("data/authors.csv")
    # main()
