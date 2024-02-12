import os
from keybert import KeyBERT
from textblob import TextBlob
from transformers import pipeline
from langchain_community.llms import HuggingFaceHub
from langchain.chains import LLMChain
from langchain.prompts import PromptTemplate
from dotenv import load_dotenv

load_dotenv()


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
        return [TextBlob(content).noun_phrases for content in content_list]

    def get_keywords(self, content_list):
        return self.kw_model.extract_keywords(
            content_list.tolist(),
            seed_keywords=self.keywords,
        )

    def set_hf_pipeline(self, task, model):
        return pipeline(
            task=task,
            model=model,
            max_length=self.hf_max_length,
            truncation=self.hf_truncation,
        )


if __name__ == "__main__":

    os.environ["HUGGINGFACEHUB_API_TOKEN"] = os.getenv["HUGGINGFACEHUB_API_TOKEN"]
