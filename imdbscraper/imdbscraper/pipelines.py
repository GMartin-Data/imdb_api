# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import re
import sqlite3

from itemadapter import ItemAdapter
from loguru import logger


CHANGE_TO_DOLLAR = {
    "¥": 1/150,
    "₩": 0.00075,
    "€": 1.09,
    "A": 0.65,
    "£": 1.27,
    "₹": 0.012,
    "DE": 0.55494846,
    "DK": 0.15,
    "F": 0.16,
    "R": 0.2,
    "$": 1,
}

TEXT_FIELDS = ("id", "kind", "title", "original_title", "genres",
               "synopsis", "audience", "casting", "countries")
INTEGER_FIELDS = ("duration_s", "release_year", "vote_count", "metacritic_score")
MONEY_FIELDS = ("budget", "worldwide_gross")
MONEY_PATTERN = r"(?P<currency>\D{1,3})(?P<amount>\S*)"


class CleanArtworkPipeline:
    @logger.catch
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        # field_names = adapter.field_names()

        # Convert rating to float
        rating = adapter.get("rating")
        try:
            adapter["rating"] = float(rating)
        except TypeError:
            adapter["rating"] = None

        # Ensure conversion to integer for integer fields
        for field_name in INTEGER_FIELDS:
            value = adapter.get(field_name)
            try:
                adapter[field_name] = int(value)
            except TypeError:
                adapter[field_name] = None

        # Clean trailing spaces in textual fields
        for field_name in TEXT_FIELDS:
            value = adapter.get(field_name)
            try:
                adapter[field_name] = value.strip()
            except AttributeError:
                adapter[field_name] = None

        # Clean budget & gross and transtype to float, eventually converting to $
        for field_name in MONEY_FIELDS:
            value = adapter.get(field_name)
            try:
                match = re.search(MONEY_PATTERN, value)
                currency = match["currency"]
                amount = match["amount"]
                try:
                    amount = amount.replace(",", "")
                    amount = int(amount) * CHANGE_TO_DOLLAR[currency]
                    adapter[field_name] = round(amount)
                except Exception as e:
                    logger.error(f"{type(e)}: {e}")
            except TypeError:
                adapter[field_name] = None

        return item


class StoreSQLitePipeline:
    def __init__(self):
        self.con = sqlite3.connect("imdb.db")
        self.cur = self.con.cursor()
        self.create_table()

    @logger.catch
    def create_table(self):
        self.cur.execute("""
                         CREATE TABLE IF NOT EXISTS films(
                            id TEXT PRIMARY KEY ,
                            kind TEXT,
                            title TEXT,
                            original_title TEXT,
                            genres TEXT,
                            duration_s INTEGER,
                            release_year INTEGER,
                            end_year INTEGER,
                            rating REAL,
                            vote_count INTEGER,
                            metacritic_score INTEGER,
                            audience TEXT,
                            countries TEXT,
                            budget INTEGER,
                            worldwide_gross INTEGER,
                            casting TEXT,
                            synopsis TEXT,
                            poster_link TEXT
                         )
                         """)
    
    @logger.catch
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        self.cur.execute(
            """
            INSERT INTO films (
            id, kind, title, original_title, genres, duration_s,
            release_year, end_year, rating, vote_count, metacritic_score,
            audience, countries, budget, worldwide_gross,
            casting, synopsis, poster_link
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                adapter.get("id"),
                adapter.get("kind"),
                adapter.get("title"),
                adapter.get("original_title"),
                adapter.get("genres"),
                adapter.get("duration_s"),
                adapter.get("release_year"),
                adapter.get("end_year"),
                adapter.get("rating"),
                adapter.get("vote_count"),
                adapter.get("metacritic_score"),
                adapter.get("audience"),
                adapter.get("countries"),
                adapter.get("budget"),
                adapter.get("worldwide_gross"),
                adapter.get("casting"),
                adapter.get("synopsis"),
                adapter.get("poster_link")
            )
        )
        self.con.commit()
        return item
    
    @logger.catch
    def close_spider(self, spider):
        self.cur.close()
        self.con.close()   