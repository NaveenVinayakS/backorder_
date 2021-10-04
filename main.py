#from training_Validation_Insertion import train_validation
#train_validation(r'C:\Users\z030590\OneDrive - Alliance\Desktop\Personal\BackOrder_Prediction\Project\Training_Batch_Files')

import streamlit as st
import os
import time
import pandas as pd
from pandas_profiling import ProfileReport
import numpy as np
import webbrowser
from prediction_Validation_Insertion import pred_validation
from predictFromModel import prediction

st.title("Backorder Prediction")

file_to_be_uploaded = st.file_uploader("Choose a file", type=['csv'])

RAWDATAPATH = r"C:\Users\z030590\OneDrive - Alliance\Desktop\Personal\BackOrder_Prediction"
                                        
def save_uploadedfile(uploadedfile):
    global RAWDATAPATH
    with open(os.path.join(RAWDATAPATH, uploadedfile.name), "wb") as f:
        
        f.write(uploadedfile.getbuffer())

        with st.spinner(text="Uploading . . ."):
            time.sleep(3)

    back_order = os.path.join(RAWDATAPATH, uploadedfile.name)

    print(back_order)

    return back_order
                                        
if st.button("Upload"):
    if file_to_be_uploaded is not None:
    
        input_files = save_uploadedfile(file_to_be_uploaded)
        st.write(f"File is uploaded in {input_files} path")
        df = pd.read_csv(file_to_be_uploaded) 
        profile = ProfileReport(df, title="Basic EDA")
        profile.to_file("output.html") 
        filename = 'output.html'
        webbrowser.open_new_tab(filename)
        print(df)      


        pred_val = pred_validation(r'C:\Users\z030590\OneDrive - Alliance\Desktop\Personal\BackOrder_Prediction\Project\Prediction_Batch_files')
        pred_val.pred_validation()
        pred = prediction(r'C:\Users\z030590\OneDrive - Alliance\Desktop\Personal\BackOrder_Prediction\Project\Prediction_Batch_files')
        path = pred.predictionFromModel()
