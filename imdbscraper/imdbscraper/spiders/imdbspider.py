import json
import urllib.parse

import scrapy

from imdbscraper.items import FilmItem


API_HEADERS = {
        'Accept': 'application/graphql+json, application/json',
        'Content-Type': 'application/json',
}

WEB_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.5',
}


class MovieApiSpider(scrapy.Spider):
    name = "movie_api"
    allowed_domains = ['caching.graphql.imdb.com']
    start_urls = ['https://caching.graphql.imdb.com/']
    counter = 0
    limit = 250  # For testing purposes

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
                "anyTitleTypeIds": ["movie"]
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
        url = f"{self.api_url}?operationName=AdvancedTitleSearch&variables={variables_encoded}&extensions={extensions_encoded}"

        yield scrapy.Request(url, headers=API_HEADERS, callback=self.parse_api_response)


    def parse_api_response(self, response):
        BASE_URL = "https://www.imdb.com/title/"
        json_resp = response.json()
        data = json_resp['data']['advancedTitleSearch']

        # Extract film data
        items = data['edges']
        for item in items:
            if self.counter < self.limit:
                self.counter += 1
                film = item['node']['title']
                # Special case of 'genres'
                genres = film['titleGenres']['genres']
                api_data = {
                    'id': film['id'],
                    'title': film['titleText']['text'],
                    'original_title': film['originalTitleText']['text'],
                    'genres': ', '.join([genre['genre']['text'] for genre in genres]),
                    'duration_s': film['runtime']['seconds'],
                    'release_year': film['releaseYear']['year'],
                    'synopsis': film['plot']['plotText']['plainText'],
                    'rating': film['ratingSummary']['aggregateRating'],
                    'vote_count': film['ratingSummary']['voteCount'],
                    'metacritic_score': film['metacritic']['metascore']['score'],
                    'poster_link': film['primaryImage']['url']
                }
                film_page_url = f"{BASE_URL}/{film['id']}"

                # Pass film-specific data to the film page to scrape
                yield scrapy.Request(
                    film_page_url,
                    headers = WEB_HEADERS,
                    callback = self.parse_film_page,
                    meta = {'api_data': api_data},
                )
            else:
                # Eventually, implement a printing or a logging message
                break

        # Extract pagination data
        has_next_page = data['pageInfo']['hasNextPage']
        end_cursor = data['pageInfo']['endCursor'] if has_next_page else None

        # If there's a next page, schedule the next API call
        if has_next_page:
            yield from self.schedule_next_api_call(end_cursor)
        

    def parse_film_page(self, response):
        # Retrieve API film-specific data passed via meta
        api_data = response.meta['api_data']

        # Scrape additional data from the film page
        TOP_INFO = "h1[data-testid='hero__pageTitle'] ~ ul"
        scraped_data = {
            'audience': response.css(f"{TOP_INFO} li:nth-child(2) > a ::text").get(),
            # 'casting': # TO DO,
        }

        # Output an item
        film_item = FilmItem()
        film_item['id'] = api_data.get("id")
        film_item['title'] = api_data.get("title")
        film_item['original_title'] = api_data.get("original_title")
        film_item['genres'] = api_data.get("genres")
        film_item['duration_s'] = api_data.get("duration_s")
        film_item['release_year'] = api_data.get("release_year")
        film_item['synopsis'] = api_data.get("synopsis")
        film_item['rating'] = api_data.get("rating")
        film_item['vote_count'] = api_data.get("vote_count")
        film_item['metacritic_score'] = api_data.get("metacritic_score")
        film_item['poster_link'] = api_data.get("poster_link")
        film_item['audience'] = api_data.get("audience")
        # film_item['casting'] = api_data.get("casting")

        yield film_item

    def schedule_next_api_call(self, end_cursor):
        # Forge the next API request URL using the cursor
        variables = {
            # Keeping the same parameters as in start_requests
            "first": 50,  # Assuming you want the first 50 items
            "locale": "en-US",
            "sortBy": "POPULARITY",
            "sortOrder": "ASC",
            "titleTypeConstraint": {
                "anyTitleTypeIds": ["movie"]
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
        next_api_url = f"{self.api_url}?operationName=AdvancedTitleSearch&variables={variables_encoded}&extensions={extensions_encoded}"

        yield scrapy.Request(next_api_url, headers=API_HEADERS, callback=self.parse_api_response)
