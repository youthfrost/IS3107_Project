import streamlit as st
import numpy as np
import pandas as pd
import joblib
from tensorflow.keras.models import load_model
import tempfile
from google.cloud import storage
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "../aesthetic-nova-454803-r7-94e7eb0af61c.json"

# GCS config
# We will be calling the GCS bucket which we have stored our trained model as well as fitted scalar, ohe, tfidf
BUCKET_NAME = "youtube-trending-videos-dataset"
GCS_MODEL_DIR = "trained_models"

# Initialize GCS client
storage_client = storage.Client()

def download_blob_to_temp(bucket_name, source_blob_name, suffix=""):
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    blob.download_to_filename(temp_file.name)
    return temp_file.name

model_path = download_blob_to_temp(BUCKET_NAME, f"{GCS_MODEL_DIR}/tuned_NN_model.h5", suffix=".h5")
scaler_path = download_blob_to_temp(BUCKET_NAME, f"{GCS_MODEL_DIR}/scaler.pkl")
ohe_path = download_blob_to_temp(BUCKET_NAME, f"{GCS_MODEL_DIR}/ohe.pkl")
tfidf_path = download_blob_to_temp(BUCKET_NAME, f"{GCS_MODEL_DIR}/tfidf.pkl")

model = load_model(model_path)
scaler = joblib.load(scaler_path)
ohe = joblib.load(ohe_path)
tfidf = joblib.load(tfidf_path)

st.title("📊 YouTube Popularity Predictor")

# User inputs
published_month = st.selectbox("Published Month", list(range(1, 13)))
tagCount = st.number_input("Tag Count", min_value=0, max_value=100)

published_dayOfWeek = st.selectbox("Day of Week", ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
categoryId = st.selectbox("Category ID", ['Film & Animation', 'Autos & Vehicles', 'Music', 'Pets & Animals', 'Sports', 'Short Movies', 'Travel & Events', 'Gaming', 'Videoblogging', 'People & Blogs', 'Comedy', 'Entertainment', 'News & Politics', 'Howto & Style', 'Education', 'Science & Technology', 'Nonprofits & Activism', 'Movies', 'Anime/Animation', 'Action/Adventure', 'Classics', 'Documentary', 'Drama', 'Family', 'Foreign', 'Horror', 'Sci-Fi/Fantasy', 'Thriller', 'Shorts', 'Shows', 'Trailers'])  

title = st.text_input("Video Title")
channelTitle = st.text_input("Channel Title")
tags = st.text_input("Tags")
description = st.text_area("Video Description")

if st.button("Predict"):
    df_input = pd.DataFrame([{
        'published_month': published_month,
        'tagCount': tagCount,
        'published_dayOfWeek': published_dayOfWeek,
        'categoryId': categoryId,
        'title': title,
        'channelTitle': channelTitle,
        'tags': tags,
        'description': description
    }])

    # Preprocess same way as training
    X_num = scaler.transform(df_input[['published_month', 'tagCount']])
    X_cat = ohe.transform(df_input[['published_dayOfWeek', 'categoryId']])
    
    combined_text = df_input[['title', 'channelTitle', 'tags', 'description']].fillna('').agg(' '.join, axis=1)
    X_text = tfidf.transform(combined_text).toarray()
    
    X_input = np.hstack([X_num, X_cat, X_text]).astype(np.float32)

    # Predict
    prediction = model.predict(X_input)
    predicted_class = np.argmax(prediction, axis=1)[0]

    st.success(f"Predicted Popularity Class: {predicted_class}")