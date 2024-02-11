import pandas as pd
import numpy as np
import streamlit as st
from utils.sf_conn import Config
from utils.assistant_api import Spotify_Assistant
from utils.prompt import PROMPT
import time


# create connection to SF to query the table in
conn = Config()
create_conn = conn.create_sf_conn()
cursor = create_conn.cursor()
query = "SELECT * FROM MAIN_SPOTIFY"
spotify_df = cursor.execute(query).fetch_pandas_all()
spotify_csv = spotify_df.to_csv('assistant_data/spotify_main_file')

## create any function for checking on waiting on the status for a reponse to be done processing in OpenAI
def wait_until_run_status_completed(assistant, run_id, progress_placeholder):
    start_time = time.time()
    while True:
        status = assistant.run_status(run_id)
        if status == "completed":
            progress_placeholder.text("Completed.")
            break
        elif status in {"in_progress", "queued"}:
            progress_placeholder.text(f"Status: {status}...")
            time.sleep(1)  # Sleep to throttle the updates
            cur_time = time.time()
            if cur_time - start_time >= 600:  # Timeout after 10 minutes
                progress_placeholder.text("Timed out waiting for completion.")
                break
        else:
            progress_placeholder.text(f"Unexpected status: {status}.")
            break

### Load in Spotify Assistant Class
assistant = Spotify_Assistant(assistant_id='asst_afQAdlE40WllyVRI7mDZBjha')
#assistant.upload_csv_file('assistant_data/spotify_main_file') THIS NEEDS TO GET FIGURED OUT IN A LATER RELEASE, right now it will upload a file for every question asked
## Add in Steamlit front end
# Display text
#st.title('SASS')
st.markdown("""
<style>
.centered {
  text-align: center;
}
</style>
<div class="centered">
    <h1>SASS</h1>
</div>
""", unsafe_allow_html=True)

st.subheader("(S)potify (Ass)istant", divider = 'rainbow')

st.markdown(
        "This assistant is connected to OpenAI and a file of songs that are from my spotify account and my friends. " 
            
        "There is a limited number of songs as the hope is to expose someone to an artist/genre that they may not have listened to before. "

        "View the drop down to see example questions, you should try and phrase a question based on how you feel and the current mood you are in.",
        unsafe_allow_html = True)


example_q1 = "I am getting ready for the gym and I need a song that has high energy and will get me pumped up."
example_q2 = "It has been raining the past few days and I have been in a depressed mood, what songs do you recommend."
example_q3 = "I have a final exam at the end of the week and I need to focus and consenstrait on my work, do you have a song that can help me focus."

# Empty string to hold the question initially
question_to_use = ""

# Select box with an empty default option
questions = st.selectbox('Example Questions', ["", example_q1, example_q2, example_q3, "Custom Question"])

# Text area for the custom question appears only if 'Custom Question' is selected
if questions == "Custom Question":
    custom_question = st.text_area("Enter your custom question here:")
    if custom_question:  # Check if the custom question is not empty
        question_to_use = custom_question

elif questions:
    # An example question was selected, so we update the question_to_use variable
    question_to_use = questions

status_placeholder = st.empty()
# Check if a question has been provided before proceeding
if question_to_use:
    st.write("Your question: ", question_to_use)
    
    # Placeholder for in-progress message
    with st.spinner("One moment while I process your question..."):
        
        # Start processing with the OpenAI Assistant
        status_placeholder.text('In progress...')
        assistant.attach_message_to_thread(question_to_use)
        run = assistant.start_run(override_prompt=PROMPT)
        wait_until_run_status_completed(assistant=assistant, run_id=run.id, progress_placeholder=status_placeholder)
    
    # Fetch and display the response
    response = assistant.assistant_answer()
    st.markdown(response)

else:
    # No question provided yet, display a prompt to the user
    st.write("Please select an example question or enter a custom question to continue.")


