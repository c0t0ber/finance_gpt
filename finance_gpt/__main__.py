import logging
import os
import sqlite3
from dataclasses import dataclass
from operator import itemgetter
from sqlite3 import Connection

from langchain.chains import create_sql_query_chain
from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool
from langchain_community.utilities.sql_database import SQLDatabase
from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_openai import ChatOpenAI
from pydantic.v1 import SecretStr
from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

logger = logging.getLogger(__name__)


class TableManager:
    DEFAULT_DIR = "dbs/"
    DRIVER_PREFIX = "sqlite:///"

    @classmethod
    def get_sql_database_tool(cls, name: str) -> SQLDatabase:
        return SQLDatabase.from_uri(cls.DRIVER_PREFIX + cls.get_db_name(name))

    @classmethod
    def get_db_name(cls, name: str) -> str:
        return cls.DEFAULT_DIR + name

    @classmethod
    def crate_connect(cls, name: str) -> Connection:
        return sqlite3.connect(cls.get_db_name(name))

    @classmethod
    def init_db(cls, name: str) -> None:
        conn = cls.crate_connect(name)
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS expenses (
                    id INTEGER PRIMARY KEY,
                    amount REAL NOT NULL,
                    category TEXT NOT NULL,
                    record_time DATETIME NOT NULL,
                    currency TEXT NOT NULL,
                    description TEXT NOT NULL
                )
            """
            )
            conn.commit()
        except Exception as e:
            logger.exception(e)
        finally:
            if conn:
                conn.close()


finance_gpt_sql_prompt = PromptTemplate(
    input_variables=["input", "table_info", "top_k", "additional_db_info"],
    template="""You are a SQLite expert. Given an input question, first create a syntactically correct SQLite query to run, then look at the results of the query and return the answer to the input question.
Unless the user specifies in the question a specific number of examples to obtain, query for at most {top_k} results using the LIMIT clause as per SQLite. You can order the results to return the most informative data in the database.
Never query for all columns from a table. You must query only the columns that are needed to answer the question. Wrap each column name in double quotes (") to denote them as delimited identifiers.
Pay attention to use only the column names you can see in the tables below. Be careful to not query for columns that do not exist. Also, pay attention to which column is in which table.
Pay attention to use date('now') function to get the current date, if the question involves "today".

Use the following format:

Question: Question here
SQLQuery: SQL Query to run
SQLResult: Result of the SQLQuery
Answer: Final answer here

Only use the following tables:
{table_info}

{additional_db_info}

Question: {input}""",
)

addition_db_info = """
Ты бот для записи и чтения финансовых транзакций.

Таблица с которой ты должен работать имеет следующие поля:
amount - может быть положительным или отрицательным числом, представляющим сумму денег, потраченную или заработанную пользователем
category - текстовое поле, представляющее категорию траты или дохода, например, "еда", "транспорт", "зарплата"
description - текстовое поле, представляющее описание траты или дохода, эту информацию пишет пользователь вместе с суммой, так же ты должен использовать ее для определения категории
record_time - дата и время, когда была сделана запись, при записи всегда подставляй текущую дату и время
currency - текстовое поле, представляющее валюту, в которой была сделана запись, если не указано, то по умолчанию используется IDR


Если человек пишет сумму и описание, то это значит что тебе нужно записать в базу данных эту транзакцию"""


class FinanceGPT:
    def __init__(self, llm: ChatOpenAI, db: SQLDatabase):
        self._llm = llm
        self._db = db
        self._sql_writer = create_sql_query_chain(llm, db, prompt=finance_gpt_sql_prompt)

        self._answer_prompt = PromptTemplate.from_template(
            """Ответь за вопрос пользователя проанализировав sql запрос и результат этого запроса:
Если запрос содержит INSERT, UPDATE, DELETE и в result нет ошибки то ответь "Запрос выполнен успешно"
Если запрос содержит SELECT, то проверь результат запроса и ответь на вопрос пользователя.

Question: {question}
SQL Query: {query}
SQL Result: {result}
Answer: """
        )

        self._sql_executor = QuerySQLDataBaseTool(db=db)

        self._chain_llm_with_answer = self._answer_prompt | self._llm

        self._chain_write_and_execute_sql = RunnablePassthrough.assign(
            query=self._sql_writer
        ).assign(result=itemgetter("query") | self._sql_executor)

    def write_and_execute_query(self, question: str) -> dict:
        return self._chain_write_and_execute_sql.invoke(
            {"question": question, "additional_db_info": addition_db_info}
        )

    def answer_question(self, question_with_query: dict) -> str:
        res = self._chain_llm_with_answer.invoke(question_with_query)
        return str(res.content)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.effective_chat:
        return
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="Привет! Я бот. Отправь мне что-нибудь. НЕ ПРОСИТЬ ДРОПНУТЬ БАЗУ, ПОТОМУ ЧТО ВЫ ДРОПНИТЕ ТОЛЬКО СВОЮ БАЗУ, А НЕ МОЮ. Я НЕ ДРОПАЮ БАЗЫ. ТАК ЖЕ НЕ НАДО СПАМИТЬ, Я ПЛАЧУ ДЕНЬНГИ. СПАСИБО ЗА ПОНИМАНИЕ.\nВсе что вы пишите мне, я отправлю в базу данных и потом буду отвечать на ваши вопросы.\nВы можете делать любые операции.",
    )


@dataclass
class SqlChainResult:
    query: str
    result: str
    answer: str


class Bot:
    def __init__(self, llm: ChatOpenAI):
        self._llm = llm

    def _create_chain_for_sql(self, chat_id: str) -> FinanceGPT:
        return FinanceGPT(self._llm, TableManager.get_sql_database_tool(chat_id))

    def _invoke_chain(self, question: str, chat_id: str) -> SqlChainResult:
        chain = self._create_chain_for_sql(chat_id)
        query_with_result = chain.write_and_execute_query(question)
        answer = chain.answer_question(query_with_result)
        return SqlChainResult(
            query=query_with_result["query"],
            result=query_with_result["result"],
            answer=answer,
        )

    async def handle_user_message(
        self, update: Update, context: ContextTypes.DEFAULT_TYPE
    ) -> None:
        if not update.effective_chat or not update.message or not update.message.text:
            return

        question = update.message.text
        chat_id = update.effective_chat.id
        TableManager.init_db(str(update.effective_chat.id))
        chain_result = self._invoke_chain(question, str(chat_id))
        result = f"Query: {chain_result.query}\n\n\nResult: {chain_result.result}\n\n\nAnswer: {chain_result.answer}"
        await context.bot.send_message(chat_id=chat_id, text=result)


def main(bot_token: str, open_ai_api_key: str) -> None:
    llm = ChatOpenAI(
        model="gpt-3.5-turbo",
        temperature=0,
        api_key=SecretStr(open_ai_api_key),
    )

    application = ApplicationBuilder().token(bot_token).build()
    bot = Bot(llm)

    start_handler = CommandHandler("start", start)
    application.add_handler(start_handler)
    application.add_handler(
        MessageHandler(filters.TEXT & (~filters.COMMAND), bot.handle_user_message)
    )

    logger.info("Bot started.")

    application.run_polling(timeout=60, poll_interval=5)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        main(os.environ["BOT_TOKEN"], os.environ["OPENAI_API_KEY"])
    except Exception as e:
        logger.exception(e)
