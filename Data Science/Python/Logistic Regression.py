# -*- coding: utf-8 -*-
"""
Created on Tue Oct 23 11:49:48 2018

@author: mabotula
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report

dataset = pd.read_csv('C:/Users/mabotula/Documents/Learning/advertising.csv')
#dataset.columns
#dataset.groupby('Clicked on Ad').size()

sns.set_style('whitegrid')
dataset['Age'].hist(bins=30)
plt.xlabel('Age')
sns.jointplot(x='Age',y='Area Income',data=dataset)
sns.jointplot(x='Age',y='Daily Time Spent on Site',data=dataset,color='red',kind='kde');

X = dataset[['Daily Time Spent on Site', 'Age', 'Area Income','Daily Internet Usage', 'Male']]
y = dataset['Clicked on Ad']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.33, random_state=54)

logmodel = LogisticRegression()
logmodel.fit(X_train,y_train)

predictions = logmodel.predict(X_test)

print(classification_report(y_test,predictions))
