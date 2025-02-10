import random
import aiohttp
from typing import AsyncGenerator
from datetime import datetime, timedelta
from datetime import timezone
import logging
from lxml.html import fromstring
from exorde_data import (
    Item,
    Content,
    Author,
    CreatedAt,
    Title,
    Url,
    Domain,
    ExternalId,
    ExternalParentId,
)
import re
from wordsegment import load, segment
# Load the wordsegment module
load()


global MAX_EXPIRATION_SECONDS
global SKIP_POST_PROBABILITY

LEMMY_NB_COMMUNITIES_TO_BROWSE = 10
LEMMY_DEFAULT_TOP_COMMUNITIES = 10
BASE_TIMEOUT = 5  # Base timeout for aiohttp requests (in seconds)

USER_AGENT_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
]


# Compile regular expressions outside of the function
escape_character_pattern = re.compile(r"\\.")
url_pattern = re.compile(
    r"http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\'(),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"
)


async def fetch_communities(sort):
    """
    Fetches a list of communities from the Lemmy API.

    Parameters:
    - sort (str): The sorting method for the communities.

    Returns:
    - List[dict]: A list of dictionaries representing the fetched communities.
    """
    try:
        url = f"https://lemmy.ml/api/v3/community/list?sort={sort}&limit=50"
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                headers={"User-Agent": random.choice(USER_AGENT_LIST)},
                timeout=BASE_TIMEOUT,
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return data["communities"]
                else:
                    return []
    finally:
        await session.close()


async def fetch_new_posts_from_community(community_name):
    """
    Fetches new posts from a specific community on Lemmy.

    Parameters:
    - community_name (str): The name of the community.

    Returns:
    - Optional[Any]: The fetched posts as a JSON object, or None if an error occurred.
    """
    try:
        url = f"https://lemmy.world/api/v3/post/list?community_name={community_name}&sort=New&limit=100"
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                headers={"User-Agent": random.choice(USER_AGENT_LIST)},
                timeout=BASE_TIMEOUT,
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    return None
    finally:
        await session.close()


async def fetch_comments_for_post(post_id, max_oldness):
    """
    Fetches comments for a specific post on Lemmy, filtering out comments older than a certain threshold.

    Parameters:
    - post_id (str): The ID of the post.
    - max_oldness (int): The maximum age of comments (in seconds) to consider as fresh.

    Returns:
    - Optional[List[Any]]: A list of fresh comments as dictionaries, or None if an error occurred.
    """
    try:
        url = f"https://lemmy.world/api/v3/comment/list?post_id={post_id}"
        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                headers={"User-Agent": random.choice(USER_AGENT_LIST)},
                timeout=BASE_TIMEOUT,
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    comments = data.get("comments", [])
                    fresh_comments = []
                    for comment in comments:
                        published_str = comment["comment"]["published"]
                        published_date = datetime.fromisoformat(
                            published_str.replace("Z", "+00:00")
                        )
                        if datetime.now(timezone.utc) - published_date < timedelta(
                            seconds=max_oldness
                        ):
                            fresh_comments.append(comment)
                    return fresh_comments
                else:
                    return None
    finally:
        await session.close()


DEFAULT_MIN_POST_LENGTH = 10
DEFAULT_MAXIMUM_ITEMS = 100
DEFAULT_OLDNESS_SECONDS = 3600  # 1 hour


def read_parameters(parameters):
    """
    Reads parameters from a dictionary and assigns default values if necessary.

    Parameters:
    - parameters (Dict[str, int]): A dictionary containing parameters.

    Returns:
    - Tuple[int, int, int]: A tuple containing max_oldness_seconds, maximum_items_to_collect, and min_post_length.
    """
    # Check if parameters is not empty or None
    if parameters and isinstance(parameters, dict):
        try:
            max_oldness_seconds = parameters.get(
                "max_oldness_seconds", DEFAULT_OLDNESS_SECONDS
            )
        except KeyError:
            max_oldness_seconds = DEFAULT_OLDNESS_SECONDS

        try:
            maximum_items_to_collect = parameters.get(
                "maximum_items_to_collect", DEFAULT_MAXIMUM_ITEMS
            )
        except KeyError:
            maximum_items_to_collect = DEFAULT_MAXIMUM_ITEMS

        try:
            min_post_length = parameters.get("min_post_length", DEFAULT_MIN_POST_LENGTH)
        except KeyError:
            min_post_length = DEFAULT_MIN_POST_LENGTH

    else:
        # Assign default values if parameters is empty or None
        min_post_length = DEFAULT_MIN_POST_LENGTH

    return max_oldness_seconds, maximum_items_to_collect, min_post_length


def sanitize_text(text):
    """
    Sanitize the input text by removing escape characters and URLs.

    Parameters:
        text (str): The text to sanitize.

    Returns:
        str: The sanitized text.
    """
    # Use the compiled patterns for substitution
    text = escape_character_pattern.sub("", text)
    return text


async def query(parameters: dict) -> AsyncGenerator[Item, None]:
    """
    Asynchronously queries items based on the provided parameters.

    Parameters:
    - parameters (Dict[str, int]): A dictionary containing parameters for the query.

    Yields:
    - AsyncGenerator[Item, None]: An asynchronous generator yielding items.
    """
    (max_oldness_seconds, maximum_items_to_collect, min_post_length) = read_parameters(
        parameters
    )
    logging.info(f"[LEMMY WORLD] Input parameters: {parameters}")
    yielded_items = 0  # Counter for the number of yielded items

    sorts = ["TopDay", "TopWeek", "TopMonth", "TopYear", "TopAll"]
    selected_sort = random.choice(sorts)

    communities = await fetch_communities(selected_sort)
    id_found = dict()
    if communities:
        try:
            selected_communities = random.sample(
                communities, LEMMY_NB_COMMUNITIES_TO_BROWSE
            )
            # add 2 communities from the top K to selected_communities
            if len(communities) > LEMMY_DEFAULT_TOP_COMMUNITIES:
                selected_communities += random.sample(
                    communities[:10], LEMMY_DEFAULT_TOP_COMMUNITIES
                )
            # remove duplicates from selected_communities
            selected_communities = list(
                {v["community"]["name"]: v for v in selected_communities}.values()
            )
            # print titles in a row, comma separated
            titles_str = (
                ", ".join(
                    [
                        community["community"]["title"]
                        for community in selected_communities
                    ]
                )
                + "]"
            )
            logging.info(
                "[LEMMY WORLD] Randomly selected lemmy communities : [" + titles_str
            )

            for community_name in selected_communities:

                logging.info(
                    f"[LEMMY WORLD] Fetching posts from '{community_name['community']['name']}' community..."
                )
                community_name = community_name["community"]["name"]
                posts = await fetch_new_posts_from_community(community_name)
                segmented_community_name_strs = segment(community_name)
                segmented_community_name = " ".join(segmented_community_name_strs)

                if posts and "posts" in posts:
                    for _post in posts["posts"]:
                        post = _post["post"]
                        post_id = post["id"]
                        post_url = post["ap_id"]
                        post_author_id = post["creator_id"]
                        post_date = post["published"]
                        # reformatted_content is title + content
                        post_content = segmented_community_name
                        post_title = ""
                        if "body" in post:  # if content exists
                            post_content += ". " + post["body"]
                        if "name" in post:  # if title exists
                            post_title = post["name"]
                            post_content += post_title
                        # clean it from escape characters and html tags without beautifulsoup
                        post_content = fromstring(post_content).text_content()
                        post_content = sanitize_text(post_content)
                        # if post published also <= 1 hour ago
                        if datetime.now(timezone.utc) - datetime.fromisoformat(
                            post_date.replace("Z", "+00:00")
                        ) < timedelta(seconds=max_oldness_seconds):

                            post_item_ = Item(
                                content=Content(post_content),
                                # author=Author(str(post_author_id)),
                                created_at=CreatedAt(str(post_date)),
                                domain=Domain("lemmy.world"),
                                title=Title(str(post_title)),
                                url=Url(str(post_url)),
                                external_id=ExternalId(str(post_id)),
                            )
                            if post_id not in id_found:
                                if yielded_items >= maximum_items_to_collect:
                                    logging.info(
                                        f"[Lemmy.world] Yielded {yielded_items} items. Maximum items to collect reached ({maximum_items_to_collect})"
                                    )
                                    break
                                yielded_items += 1
                                id_found[post_id] = True
                                logging.info(
                                    f"[Lemmy.world] Found NEW post: {post_item_}"
                                )
                                yield post_item_
                        comments = await fetch_comments_for_post(
                            post_id, max_oldness_seconds
                        )
                        if comments:
                            for comment in comments:
                                comment_id = comment["comment"]["id"]
                                comment_text = comment["comment"]["content"]
                                # comment_content = community + '. ' + post_tile + '. ' + comment_text

                                comment_content = (
                                    comment_text
                                )
                                comment_content = sanitize_text(comment_content)

                                comment_date = comment["comment"]["published"]
                                comment_url = comment["comment"]["ap_id"]
                                comment_parent_id = comment["comment"]["post_id"]
                                author_id = comment["comment"]["creator_id"]
                                comm_item_ = Item(
                                    content=Content(str(comment_content)),
                                    # author=Author(str(author_id)),
                                    created_at=CreatedAt(str(comment_date)),
                                    domain=Domain("lemmy.world"),
                                    title=Title(post_title),
                                    url=Url(str(comment_url)),
                                    external_id=ExternalId(str(comment_id)),
                                    external_parent_id=ExternalParentId(
                                        str(comment_parent_id)
                                    ),
                                )
                                if comment_id not in id_found:
                                    if yielded_items >= maximum_items_to_collect:
                                        logging.info(
                                            f"[Lemmy.world] Yielded {yielded_items} items. Maximum items to collect reached ({maximum_items_to_collect})"
                                        )
                                        break
                                    yielded_items += 1
                                    id_found[comment_id] = True
                                    logging.info(
                                        f"[Lemmy.world] Found NEW Comment: {comm_item_}" 
                                    )
                                    yield comm_item_
                        if yielded_items >= maximum_items_to_collect:
                            break
                if yielded_items >= maximum_items_to_collect:
                    break
        except Exception as e:
            logging.exception(
                "[Lemmy.world] Error browsing communities or fetching posts/comments. Moving on."
            )
