import streamlit as st
import pandas as pd
import sys
import os



st.write("# 🤖 Bamboo App")

st.write("")

st.write("Hey 👋, I am an AI powered app who can access ESG documents.")

st.write("I got features to answer your prompts, summarize documents, and compare the key changes between documents.")

st.write("🚀Select a functionality to get going!")

pages=os.listdir("pages")

for i in pages:
    if ".py" in i:
        file_url=f"pages/{i}"
        label=i.split("_")[1]
        label=label.replace("_"," ")
        label=label.replace(".py"," ")
        st.page_link(file_url,label=label,icon="🔗")
