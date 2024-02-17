import json
import logging
import os

from langchain_openai import ChatOpenAI
from pydantic.v1 import SecretStr
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters

from finance_gpt.bot import Bot
from finance_gpt.google_table import GoogleTable

logger = logging.getLogger(__name__)


def main(
    bot_token: str,
    open_ai_api_key: str,
    working_chats_with_topic_parsed: dict | None,
    google_spreadsheet_url: str,
) -> None:
    llm = ChatOpenAI(
        model="gpt-3.5-turbo",
        temperature=0,
        api_key=SecretStr(open_ai_api_key),
    )

    google_table = GoogleTable(google_spreadsheet_url)

    bot = Bot(llm, working_chats_with_topic_parsed, google_table)

    default_filters = filters.TEXT & (~filters.COMMAND)
    match working_chats_with_topic_parsed:
        case None:
            handler_filters = default_filters
        case _:
            handler_filters = default_filters & filters.Chat(
                chat_id=working_chats_with_topic_parsed.keys()
            )

    start_handler = CommandHandler("start", bot.start)

    application = ApplicationBuilder().token(bot_token).build()
    application.add_handler(start_handler)
    application.add_handler(MessageHandler(handler_filters, bot.handle_user_message))

    logger.info("Bot started.")

    application.run_polling(timeout=60, poll_interval=5)


def _get_working_chats_with_topic() -> dict | None:
    match working_chats_with_topic := os.environ.get(  # noqa: R503
        "WORKING_CHATS_WITH_TOPIC"
    ):
        case None:
            return None
        case _:
            return {int(k): v for k, v in json.loads(working_chats_with_topic).items()}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    try:
        main(
            os.environ["BOT_TOKEN"],
            os.environ["OPENAI_API_KEY"],
            _get_working_chats_with_topic(),
            os.environ["GOOGLE_SPREADSHEET_URL"],
        )
    except Exception as e:
        logger.exception(e)
