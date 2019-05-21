import pandas as pd
import numpy as np                     # For mathematical calculations
import seaborn as sns                  # For data visualization
import matplotlib.pyplot as plt        # For plotting graphs
%matplotlib inline
import warnings                        # To ignore any warnings
warnings.filterwarnings("ignore")

train=pd.read_csv("C:/Users/mabotula/Documents/Equinor/POC/Data/LoanPrediction/train_u6lujuX_CVtuZ9i.csv")
test=pd.read_csv("C:/Users/mabotula/Documents/Equinor/POC/Data/LoanPrediction/test_Y3wMUE5_7gLdaTN.csv")

# Backup
train_original=train.copy()
test_original=test.copy()

train.columns
test.columns
