# -*- coding: utf-8 -*-
"""
Created on Wed Oct 17 13:04:24 2018

@author: mabotula
"""
import numpy as np
import pandas as pd
from sklearn import  linear_model
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
#import matplotlib.pyplot as plt
from sklearn.metrics import accuracy_score
from sklearn.preprocessing import LabelEncoder,OneHotEncoder

# Import data using Pandas

dataset = pd.read_csv('C:/Users/mabotula/Documents/Learning/Automobile price data _Raw.csv')

# Preprocessing, Filter and select required columns

dataset = dataset[['make','body-style','wheel-base','engine-size','horsepower','peak-rpm','highway-mpg','symboling','price']]
dataset = dataset[(dataset != '?').all(axis=1)]
dataset[['horsepower','peak-rpm','price']] = dataset[['horsepower','peak-rpm','price']].apply(pd.to_numeric)
  # Encode with Labels for categeroy fields
Labelencoder_x = LabelEncoder()
dataset[['make']] = Labelencoder_x.fit_transform(dataset[['make']])
dataset[['body-style']] = Labelencoder_x.fit_transform(dataset[['body-style']])
  # One hot encoder to make the data understandable to computer( in terms of 0 and 1)
onehotencoder = OneHotEncoder(categorical_features = [1])
X = onehotencoder.fit_transform(x).toarray()

X = X[:,1:12]
#dataset.dtypes

# Select input(X) and output(Y) columns data

x = dataset.iloc[:,2:8]
y = dataset.iloc[:,8:9]

# Split the data for Train and test

x_train,x_test,y_train,y_test = train_test_split(x,y,test_size = 0.2, random_state = 1)

# Initiate Model

regr = linear_model.LinearRegression()

# train model using training data set
regr.fit(x_train, y_train)

# Make prediction
pred = regr.predict(x_test)

# The coefficients
print('Coefficients: \n', regr.coef_)
# The mean squared error
print("Mean squared error: %.2f"
      % mean_squared_error(y_test, pred))
# Explained variance score: 1 is perfect prediction
print('Variance score: %.2f' % r2_score(y_test, pred))




