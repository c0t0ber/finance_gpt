import logging
from dataclasses import dataclass

from langchain_openai import ChatOpenAI
from telegram import Update
from telegram.ext import ContextTypes

from finance_gpt.google_table import GoogleTable
from finance_gpt.gpt import FinanceGPT
from finance_gpt.sql_utils import TableManager

logger = logging.getLogger(__name__)


@dataclass
class SqlChainResult:
    query: str
    result: str
    answer: str


class Bot:
    def __init__(
        self,
        llm: ChatOpenAI,
        working_chats_with_topic_parsed: dict | None,
        google_table: GoogleTable,
    ) -> None:
        self._llm = llm
        self._working_chats_with_topic_parsed = working_chats_with_topic_parsed or {}
        self._google_table = google_table

    def _create_chain_for_sql(self, chat_id: str) -> FinanceGPT:
        return FinanceGPT(self._llm, TableManager.get_sql_database_tool(chat_id))

    def _save_to_google_table(self, result: str, query: str, chat_id: str) -> None:
        if not query.lower().startswith("insert") or "error" in result.lower():
            return
        if last_row := TableManager.get_last_row(chat_id):
            self._google_table.append_row(last_row)

    def _invoke_chain(self, question: str, chat_id: str) -> SqlChainResult:
        chain = self._create_chain_for_sql(chat_id)

        query_with_result = chain.write_and_execute_query(question)
        query = query_with_result["query"]
        result = query_with_result["result"]

        self._save_to_google_table(result, query, chat_id)

        answer = chain.answer_question(query_with_result)

        return SqlChainResult(
            query=query,
            result=result,
            answer=answer,
        )

    async def handle_user_message(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        if not update.effective_chat or not update.message or not update.message.text:
            return

        topic = None
        if allowed_topic := self._working_chats_with_topic_parsed.get(
            update.effective_chat.id
        ):
            topic = update.message.message_thread_id
            if topic != allowed_topic:
                return

        logger.info(f"Got message: {update}")

        question = update.message.text
        chat_id = update.effective_chat.id

        TableManager.init_db(str(update.effective_chat.id))

        chain_result = self._invoke_chain(question, str(chat_id))
        result = f"Query: {chain_result.query}\n\n\nResult: {chain_result.result}\n\n\nAnswer: {chain_result.answer}"

        await context.bot.send_message(
            chat_id=chat_id,
            text=result,
            message_thread_id=topic,
            reply_to_message_id=update.message.message_id,
        )

    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if not update.effective_chat:
            return
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Привет! Я бот. Отправь мне что-нибудь. НЕ ПРОСИТЬ ДРОПНУТЬ БАЗУ, ПОТОМУ ЧТО ВЫ ДРОПНИТЕ ТОЛЬКО СВОЮ БАЗУ, А НЕ МОЮ. Я НЕ ДРОПАЮ БАЗЫ. ТАК ЖЕ НЕ НАДО СПАМИТЬ, Я ПЛАЧУ ДЕНЬНГИ. СПАСИБО ЗА ПОНИМАНИЕ.\nВсе что вы пишите мне, я отправлю в базу данных и потом буду отвечать на ваши вопросы.\nВы можете делать любые операции.",
        )
