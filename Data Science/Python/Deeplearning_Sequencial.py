# -*- coding: utf-8 -*-
"""
Created on Wed Oct 24 16:09:06 2018

@author: mabotula
"""

import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder, OneHotEncoder

data = pd.read_csv('C:/Users/mabotula/Documents/Learning/DeepLearning/Capgemini_ML_DL/Churn_Modelling.csv')

data.columns
x = data.iloc[:,3:13]
y = data.iloc[:,13]

x_Labl = LabelEncoder()

x[['Geography']] = x_Labl.fit_transform(x[['Geography']])
x[['Gender']] = x_Labl.fit_transform(x[['Gender']])

x_hotenc = OneHotEncoder(categorical_features=[1])

x = x_hotenc.fit_transform(x).toarray()
x = x[:,1:12]

from sklearn.model_selection import train_test_split

x_train,x_test,y_train,y_test = train_test_split(x,y,test_size = 0.2,random_state = 0)


from sklearn.preprocessing import StandardScaler
sc = StandardScaler()
x_train = sc.fit_transform(x_train)
x_test = sc.fit_transform(x_test)

import keras
from keras.models import Sequential
from keras.layers import Dense

classifier = Sequential()

classifier.add(Dense(units = 6, kernel_initializer="uniform",activation="relu",input_dim = 11))

classifier.add(Dense(units = 6, kernel_initializer="uniform",activation="relu"))

classifier.add(Dense(units = 1,kernel_initializer="uniform",activation = "sigmoid"))

classifier.compile(optimizer="adam",loss = "binary_crossentropy",metrics = ['accuracy'])

classifier.fit(x_train,y_train,batch_size=10,epochs =20)

predict = classifier.predict(x_test)

predict = (predict > 0.5)

from sklearn.metrics import confusion_matrix

cf = confusion_matrix(y_test,predict)

cf


(1550+130)/(1550+45+275+130)










