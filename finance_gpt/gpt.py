from operator import itemgetter

from langchain.chains import create_sql_query_chain
from langchain_community.tools.sql_database.tool import QuerySQLDataBaseTool
from langchain_community.utilities.sql_database import SQLDatabase
from langchain_core.prompts import PromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_openai import ChatOpenAI

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
