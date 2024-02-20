import os
from utils.prompt import PROMPT
from openai import OpenAI
import pandas as pd
import streamlit as st
import pyarrow



class Spotify_Assistant():

    def __init__(self, assistant_id, thread_id=None):
        self.client = OpenAI() # this is also the default, it can be omitted)
        self.thread = self.client.beta.threads.create() if not thread_id else self.client.beta.threads.retrieve(thread_id)
        self.assistant_id = assistant_id

    # @st.cache_data()
    def upload_csv_file(self, file_path):
        # #List existing files
        # existing_files = self.client.files.list()

        # #Find and delete the file(s) related to 'assistants' purpose
        # for existing_file in existing_files:
        #     if existing_file.purpose == 'assistants':
        #         self.client.files.delete(existing_file.id)

        with open(file_path, "rb") as file:
            uploaded_file = self.client.files.create(
                file=file,
                purpose='assistants'
            )

       # add to assistant
        add_file_to_assistant = self.client.beta.assistants.files.create(
            assistant_id = self.assistant_id,
            file_id = uploaded_file.id
        )
        return add_file_to_assistant.id


    def attach_message_to_thread(self, user_question):
        self.client.beta.threads.messages.create(
            self.thread.id,
            role="user",
            content=user_question
        )

    def start_run(self, override_prompt=None):
        ast = self.client.beta.assistants.retrieve(self.assistant_id)
        prompt = override_prompt if override_prompt else ast.instructions
        run = self.client.beta.threads.runs.create(
            thread_id=self.thread.id,
            assistant_id=self.assistant_id,
            instructions=prompt
        )
        return run
    
    def run_status(self, run_id):
        run = self.client.beta.threads.runs.retrieve(
                thread_id=self.thread.id,
                run_id=run_id
        )
        return run.status
    
            
    def assistant_answer(self, return_all_messages_in_thread=False):
        response = self.client.beta.threads.messages.list(thread_id=self.thread.id)
        if return_all_messages_in_thread:
            return str(response)
        return response.data[0].content[0].text.value
    