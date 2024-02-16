import logging
from keybert import KeyBERT
from textblob import TextBlob
from transformers import pipeline
from langchain_openai import ChatOpenAI
from langchain.output_parsers.openai_functions import JsonKeyOutputFunctionsParser
from langchain.chains.summarize import load_summarize_chain
from langchain.docstore.document import Document
from langchain.text_splitter import CharacterTextSplitter
from langchain.prompts import PromptTemplate
import time as tme
from contextlib import contextmanager
from dotenv import dotenv_values
import tiktoken

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

test = dotenv_values("C:\\Users\\Brett\\OneDrive\\Desktop\\RTP-Radar\\.env")


@contextmanager
def timer(label):
    start = tme.time()
    try:
        yield
    finally:
        end = tme.time()
    time = round(end - start, 2)
    logger.info(f"{label}: {time} seconds")


class ContentExtender:
    def __init__(self):
        self.keywords = [
            "raleigh",
            "chapel hill",
            "bull city",
            "durham",
            "triangle",
            "north carolina",
            "nc",
            "carolina",
            "fayetteville",
            "crabtree",
        ]
        self.kw_model = KeyBERT()
        self.hf_max_length = 512
        self.hf_truncation = True

    def get_nouns(self, content_list):
        with timer("Extracting Nouns from Content"):
            return [TextBlob(content).noun_phrases for content in content_list]

    def get_keywords(self, content_list):
        with timer("Extracting Keywords from Content"):
            return self.kw_model.extract_keywords(
                content_list.tolist(),
                seed_keywords=self.keywords,
            )

    def set_hf_pipeline(self, task, model):
        with timer(f"Loading HuggingFace Pipeline for {task}"):
            return pipeline(
                task=task,
                model=model,
                max_length=self.hf_max_length,
                truncation=self.hf_truncation,
            )

    def classify_emotions(self, content_list):
        with timer("Classifying Emotions from Content"):
            classifier = self.set_hf_pipeline(
                "text-classification", "SamLowe/roberta-base-go_emotions"
            )
            return classifier(content_list)


class ContentSummarizer:
    def __init__(self):
        self.template = """
        You are a new reporter for news related to the Research Triangle Park area in North Carolina. Your job is to summarize articles and reddit posts that originate from the Research Triangle Park area.
        For every 250-300 word summary outlining the main points of each article or reddit post you will get paid an additional $200. 
        If the article or reddit post is less than 250 words then don't waste your time writing a summary. 
        Please write a summary of the following piece of content:

        {article} 
        """
        self.prompt = PromptTemplate.from_template(self.template)
        self.openai_modelname = "gpt-3.5-turbo-0125"
        self.model = ChatOpenAI(
            openai_api_key=test["OPENAI_API_KEY"],
            model_name=self.openai_modelname,
            temperature=0.5,
        )
        self.text_splitter = CharacterTextSplitter.from_tiktoken_encoder(
            model_name=self.openai_modelname
        )
        self.verbose = True
        self.functions = [
            {
                "name": "summarize",
                "description": "A summary of the content",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "summary": {
                            "type": "string",
                            "description": "The summary of the content",
                        },
                    },
                    "required": ["summary"],
                },
            }
        ]
        # self.chain = (
        #     self.prompt
        #     | self.model.bind(
        #         function_call={"name": "summarize"}, functions=self.functions
        #     )
        #     | JsonKeyOutputFunctionsParser(key_name="summary")
        # )

    def get_summaries(self, content):
        with timer("Generating Summaries of Content"):
            chain = self.get_chain()
            if type(content) != list:
                content_dict = {"article": content}
            else:
                # ADD CODE HERE TO HANDLE WHEN CONTENT IS A LIST OF DOCUMENTS
            return chain.invoke(content_dict)

    def num_tokens_from_string(self, string: str) -> int:
        with timer("Calculating Number of Tokens"):
            encoding = tiktoken.encoding_for_model(self.openai_modelname)
            num_tokens = len(encoding.encode(string))
            return num_tokens

    def set_chain(self, chain_type):
        with timer(f"Setting {chain_type} Chain"):
            if chain_type == "stuff":
                self.chain = load_summarize_chain(
                    self.model, chain_type=chain_type, prompt=self.prompt
                )
            else:
                self.chain = load_summarize_chain(
                    self.model,
                    chain_type=chain_type,
                    map_prompt=self.prompt,
                    combine_prompt=self.prompt,
                )

    def get_chain(self):
        return self.chain

    def make_docs(self, content):
        with timer("Splitting Long content into multiple docs"):
            texts = self.text_splitter.split_text(content)

            return [Document(page_content=t) for t in texts]
