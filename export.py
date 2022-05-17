import argparse
import csv
import functools
import itertools
import json
import logging
import math
import os
import time
from typing import Callable, List, Tuple, Type

# https://github.com/prius/python-leetcode
import leetcode.api.default_api  # type: ignore
import leetcode.api_client  # type: ignore
import leetcode.auth  # type: ignore
import leetcode.configuration  # type: ignore
import leetcode.models.graphql_query  # type: ignore
import leetcode.models.graphql_query_problemset_question_list_variables  # type: ignore
import leetcode.models.graphql_query_problemset_question_list_variables_filter_input  # type: ignore
import leetcode.models.graphql_question_detail  # type: ignore
import urllib3  # type: ignore
from tqdm import tqdm  # type: ignore

logging.getLogger().setLevel(logging.INFO)


def _get_leetcode_api_client() -> leetcode.api.default_api.DefaultApi:
    """
    Leetcode API instance constructor.

    This is a singleton, because we don't need to create a separate client
    each time
    """

    configuration = leetcode.configuration.Configuration()

    session_id = os.environ["LEETCODE_SESSION_ID"]
    csrf_token = leetcode.auth.get_csrf_cookie(session_id)

    configuration.api_key["x-csrftoken"] = csrf_token
    configuration.api_key["csrftoken"] = csrf_token
    configuration.api_key["LEETCODE_SESSION"] = session_id
    configuration.api_key["Referer"] = "https://leetcode.com"
    configuration.debug = False
    api_instance = leetcode.api.default_api.DefaultApi(
        leetcode.api_client.ApiClient(configuration)
    )

    return api_instance


def retry(times: int, exceptions: Tuple[Type[Exception]], delay: float) -> Callable:
    """
    Retry Decorator
    Retries the wrapped function/method `times` times if the exceptions listed
    in `exceptions` are thrown
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(times - 1):
                try:
                    return func(*args, **kwargs)
                except exceptions:
                    logging.exception(
                        "Exception occured, try %s/%s", attempt + 1, times
                    )
                    time.sleep(delay)

            logging.error("Last try")
            return func(*args, **kwargs)

        return wrapper

    return decorator


@retry(times=3, exceptions=(urllib3.exceptions.ProtocolError,), delay=5)
def _get_problems_count() -> int:
    api_instance = _get_leetcode_api_client()

    graphql_request = leetcode.models.graphql_query.GraphqlQuery(
        query="""
        query problemsetQuestionList($categorySlug: String, $limit: Int, $skip: Int, $filters: QuestionListFilterInput) {
          problemsetQuestionList: questionList(
            categorySlug: $categorySlug
            limit: $limit
            skip: $skip
            filters: $filters
          ) {
            totalNum
          }
        }
        """,
        variables=leetcode.models.graphql_query_problemset_question_list_variables.GraphqlQueryProblemsetQuestionListVariables(
            category_slug="",
            limit=1,
            skip=0,
            filters=leetcode.models.graphql_query_problemset_question_list_variables_filter_input.GraphqlQueryProblemsetQuestionListVariablesFilterInput(
                tags=[],
                # difficulty="MEDIUM",
                # status="NOT_STARTED",
                # list_id="7p5x763",  # Top Amazon Questions
                # premium_only=False,
            ),
        ),
        operation_name="problemsetQuestionList",
    )

    time.sleep(2)  # Leetcode has a rate limiter
    data = api_instance.graphql_post(body=graphql_request).data

    return data.problemset_question_list.total_num or 0


@retry(times=3, exceptions=(urllib3.exceptions.ProtocolError,), delay=5)
def _get_problems_data_page(
    offset: int, page_size: int, page: int
) -> List[leetcode.models.graphql_question_detail.GraphqlQuestionDetail]:
    api_instance = _get_leetcode_api_client()

    graphql_request = leetcode.models.graphql_query.GraphqlQuery(
        query="""
        query problemsetQuestionList($categorySlug: String, $limit: Int, $skip: Int, $filters: QuestionListFilterInput) {
          problemsetQuestionList: questionList(
            categorySlug: $categorySlug
            limit: $limit
            skip: $skip
            filters: $filters
          ) {
            questions: data {
                questionFrontendId
                title
                titleSlug
                categoryTitle
                frequency
                isPaidOnly
                topicTags {
                  name
                  slug
                }
                companyTagStats
            }
          }
        }
        """,
        variables=leetcode.models.graphql_query_problemset_question_list_variables.GraphqlQueryProblemsetQuestionListVariables(
            category_slug="",
            limit=page_size,
            skip=offset + page * page_size,
            filters=leetcode.models.graphql_query_problemset_question_list_variables_filter_input.GraphqlQueryProblemsetQuestionListVariablesFilterInput(),
        ),
        operation_name="problemsetQuestionList",
    )

    time.sleep(2)  # Leetcode has a rate limiter
    data = api_instance.graphql_post(
        body=graphql_request
    ).data.problemset_question_list.questions

    return data


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments for the script
    """
    parser = argparse.ArgumentParser(
        description="Fetch leetcode problems and output them to a CSV file"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        help="Fetch this many problems at once (set less if leetcode times out)",
        default=300,
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Write output to file",
        default="problems.csv",
    )

    args = parser.parse_args()

    return args


def _get_problems_data(
    page_size: int,
) -> List[leetcode.models.graphql_question_detail.GraphqlQuestionDetail]:
    problem_count = _get_problems_count()

    start = 0
    stop = problem_count

    problems: List[leetcode.models.graphql_question_detail.GraphqlQuestionDetail] = []

    logging.info(f"Fetching {stop - start + 1} problems {page_size} per page")

    for page in tqdm(
        range(math.ceil((stop - start + 1) / page_size)),
        unit="problem",
        unit_scale=page_size,
    ):
        data = _get_problems_data_page(start, page_size, page)
        problems.extend(data)

    return problems


def main() -> None:
    args = parse_args()
    problems_data = _get_problems_data(args.batch_size)

    csv_header = [
        "Question id",
        "title",
        "slug",
        "category",
        "frequency",
        "is_paid",
        "topics",
        "companies",
    ]
    with open(args.output, "w") as csv_file:
        csv_writer = csv.writer(csv_file, delimiter=";")
        csv_writer.writerow(csv_header)

        for problem_data in problems_data:
            csv_writer.writerow(
                [
                    problem_data.question_frontend_id,
                    problem_data.title,
                    problem_data.title_slug,
                    problem_data.category_title,
                    problem_data.frequency,
                    problem_data.is_paid_only,
                    ",".join([d.slug for d in problem_data.topic_tags]),
                    ",".join(
                        {
                            d["slug"]
                            for d in itertools.chain(
                                *json.loads(problem_data.company_tag_stats).values()
                            )
                        }
                    ),
                ]
            )


if __name__ == "__main__":
    main()
