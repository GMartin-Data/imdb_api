import json
import urllib.parse

from loguru import logger
import scrapy

from imdbscraper.items import ArtworkItem


API_HEADERS = {
        'Accept': 'application/graphql+json, application/json',
        'Content-Type': 'application/json',
}

WEB_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.5',
}


class ArtworkApiSpider(scrapy.Spider):
    name = "artwork_api"
    allowed_domains = ['caching.graphql.imdb.com', 'imdb.com']
    start_urls = ['https://caching.graphql.imdb.com/']
    running = True

    def __init__(self, kind: str = "movie", limit: int = 50, *args, **kwargs):
        """
        The possible values for `kind` are "movie" (default), "tvSeries", or "movie,tvSeries"
        """
        super(ArtworkApiSpider, self).__init__(*args, **kwargs)
        self.kind = kind.split(",")
        self.limit = int(limit)
        self.counter = 0

    @logger.catch
    def start_requests(self):
        # Adjusted variables without the "after" parameter for the initial request
        variables = {
            "filter": {
                # Your filters here, if needed, otherwise keep as empty objects
            },
            "first": 50,  # Assuming you want the first 50 items
            "locale": "en-US",
            "sortBy": "POPULARITY",
            "sortOrder": "ASC",
            "titleTypeConstraint": {
                "anyTitleTypeIds": self.kind
            }
            # Other parameters as needed
        }

        extensions = {
            "persistedQuery": {
                "sha256Hash": "65dd1bac6fea9c75c87e2c0435402c1296b5cc5dd908eb897269aaa31fff44b1",
                "version": 1
            }
        }

        # URL-encode the variables and extensions as before
        variables_encoded = urllib.parse.quote(json.dumps(variables))
        extensions_encoded = urllib.parse.quote(json.dumps(extensions))

        # Construct the full API URL
        url = f"{self.start_urls[0]}?operationName=AdvancedTitleSearch&variables={variables_encoded}&extensions={extensions_encoded}"

        yield scrapy.Request(url, headers=API_HEADERS, callback=self.parse_api_response)


    @logger.catch
    def parse_api_response(self, response):
        BASE_URL = "https://www.imdb.com/title/"
        json_resp = response.json()
        data = json_resp['data']['advancedTitleSearch']

        # Extract artwork data
        items = data['edges']
        for idx, item in enumerate(items):
            if self.counter < self.limit:
                self.counter += 1
                artwork = item['node']['title']
                logger.info(f"GOT idx: {idx} for artwork of id: {artwork.get('id', 'missing')}")

                ## Implement hard logic for complex keys
                # kind
                if (title_type := artwork.get('titleType', {})) is not None:
                    kind = title_type.get('text')
                else:
                    kind = None

                # title
                if (title_text := artwork.get('titleText', {})) is not None:
                    title = title_text.get('text')
                else:
                    title = None
                 
                # original_title
                if (original_title_text := artwork.get('originalTitleText', {})) is not None:
                    original_title = original_title_text.get('text')
                else:
                    original_title = None
                    
                # Special case of 'genres'
                genres = artwork.get('titleGenres', {}).get('genres', [])
                genres = ', '.join([genre.get('genre', {}).get('text', '') for genre in genres])

                # duration_s
                if (runtime := artwork.get('runtime', {})) is not None:
                    duration_s = runtime.get('seconds')
                else:
                    duration_s = None

                # release_year
                if (release := artwork.get('releaseYear', {})) is not None:
                    release_year = release.get('year')
                else:
                    release_year = None

                # synopsis
                if (plot := artwork.get('plot', {})) is None:
                    synopsis = None
                else:
                    if (plot_text := plot.get('plotText', {})) is None:
                        synopsis = None
                    else:
                        synopsis = plot_text.get('plainText')
                
                # rating & vote_count
                if (ratings_summary := artwork.get('ratingsSummary', {})) is not None:
                    rating = ratings_summary.get('aggregateRating')
                    vote_count = ratings_summary.get('voteCount')
                else:
                    rating = vote_count = None
                    
                # metacritic_score
                if (metacritic := artwork.get('metacritic', {})) is None:
                    metacritic_score = None
                else:
                    if (metascore := metacritic.get('metascore', {})) is None:
                        metacritic_score = None
                    else:
                        metacritic_score = metascore.get('score')
                
                # poster_link
                if (primary_image := artwork.get('primaryImage', {})) is not None:
                    poster_link = primary_image.get('url')
                else:
                    poster_link = None            

                api_data = {
                    'id': artwork.get('id', 'missing'),
                    'kind': kind,
                    'title': title,
                    'original_title': original_title,
                    'genres': genres,
                    'duration_s': duration_s,
                    'release_year': release_year,
                    'synopsis': synopsis,
                    'rating': rating,  # Implement np.nan?
                    'vote_count': vote_count,  # Implement np.nan?
                    'metacritic_score': metacritic_score,  # Implement np.nan?
                    'poster_link': poster_link,
                }
                artwork_page_url = f"{BASE_URL}{artwork['id']}"

                # Pass artwork-specific data to the artwork page to scrape
                yield scrapy.Request(
                    artwork_page_url,
                    headers = WEB_HEADERS,
                    callback = self.parse_artwork_page,
                    meta = {'api_data': api_data},
                )
            else:
                # Eventually, implement a printing or a logging message
                self.running = False
                break
        
        if self.running:
            # Extract pagination data
            has_next_page = data.get('pageInfo', {}).get('hasNextPage', False)
            end_cursor = data.get('pageInfo', {}).get('endCursor', '') if has_next_page else None  # SETTING endCursor to '' may cause problems.

            # If there's a next page, schedule the next API call
            if has_next_page:
                yield from self.schedule_next_api_call(end_cursor)


    @logger.catch
    def parse_artwork_page(self, response):
        # Retrieve API artwork-specific data passed via meta
        api_data = response.meta['api_data']

        ## Scrape additional data from the artwork page
        # audience
        TOP_INFO = "h1[data-testid='hero__pageTitle'] ~ ul"
        if api_data['kind'] == "Movie":
            audience = response.css(f"{TOP_INFO} li:nth-child(2) > a ::text").get()
        else:
            audience = response.css(f"{TOP_INFO} li:nth-child(3) > a ::text").get()
        # budget & gross
        money = response.css("div[data-testid='title-boxoffice-section'] ul li span::text").getall()
        logger.debug(f"{money = }")
        try:
            budget = money[1]
            worldwide_gross = money[-1]
        except Exception as e:
            budget = None
            worldwide_gross = None

        scraped_data = {
            'audience': audience,
            'casting': ', '.join(response.css("a[data-testid='title-cast-item__actor']::text").getall()),
            'countries': ', '.join(response.css("li[data-testid='title-details-origin'] a::text").getall()),
            'budget': budget,
            'worldwide_gross': worldwide_gross,
        }

        # Output an item
        artwork_item = ArtworkItem()
        artwork_item["id"] = api_data.get("id")
        artwork_item["kind"] = api_data.get("kind")
        artwork_item["title"] = api_data.get("title")
        artwork_item["original_title"] = api_data.get("original_title")
        artwork_item["genres"] = api_data.get("genres")
        artwork_item["duration_s"] = api_data.get("duration_s")
        artwork_item["release_year"] = api_data.get("release_year")
        artwork_item["synopsis"] = api_data.get("synopsis")
        artwork_item["rating"] = api_data.get("rating")
        artwork_item["vote_count"] = api_data.get("vote_count")
        artwork_item["metacritic_score"] = api_data.get("metacritic_score")
        artwork_item["poster_link"] = api_data.get("poster_link")
        artwork_item["audience"] = scraped_data.get("audience")
        artwork_item["casting"] = scraped_data.get("casting")
        artwork_item["countries"] = scraped_data.get("countries")
        artwork_item["budget"] = scraped_data.get("budget")
        artwork_item["worldwide_gross"] = scraped_data.get("worldwide_gross")

        yield artwork_item


    @logger.catch
    def schedule_next_api_call(self, end_cursor):
        # Forge the next API request URL using the cursor
        variables = {
            # Keeping the same parameters as in start_requests
            "first": 50,  # Assuming you want the first 50 items
            "locale": "en-US",
            "sortBy": "POPULARITY",
            "sortOrder": "ASC",
            "titleTypeConstraint": {
                "anyTitleTypeIds": self.kind
            },
            # Adding end_cursor to delimit request
            "after": end_cursor,
        }
        extensions = {
        "persistedQuery": {
            "sha256Hash": "65dd1bac6fea9c75c87e2c0435402c1296b5cc5dd908eb897269aaa31fff44b1",
            "version": 1
        }
        }
        # URL-encode the variables and extensions as before
        variables_encoded = urllib.parse.quote(json.dumps(variables))
        extensions_encoded = urllib.parse.quote(json.dumps(extensions))

        # Construct the full URL as before
        next_api_url = f"{self.start_urls[0]}?operationName=AdvancedTitleSearch&variables={variables_encoded}&extensions={extensions_encoded}"

        yield scrapy.Request(next_api_url, headers=API_HEADERS, callback=self.parse_api_response)
