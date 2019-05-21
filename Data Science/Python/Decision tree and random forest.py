# -*- coding: utf-8 -*-
"""
Created on Tue Oct 23 12:13:53 2018

@author: mabotula
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeClassifier
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.ensemble import RandomForestClassifier

dataset = pd.read_csv('C:/Users/mabotula/Documents/Learning/loan_data.csv')
dataset.columns

cat_feature = ['purpose']
final_data = pd.get_dummies(dataset,columns=cat_feature,drop_first = True )

dataset.groupby('not.fully.paid').size()
x = final_data.drop('not.fully.paid',axis = 1)
y = final_data['not.fully.paid']


x_train,x_test,y_train,y_test = train_test_split(x,y, test_size = 0.30, random_state = 101)

dtree = DecisionTreeClassifier()
dtree.fit(x_train,y_train)

predictions = dtree.predict(x_test)

print(classification_report(y_test,predictions))
print(confusion_matrix(y_test,predictions))
(1996+99)/(1996+435+344+99)

Randforest = RandomForestClassifier(n_estimators=600)
Randforest.fit(x_train,y_train)

predictions = Randforest.predict(x_test)

print(classification_report(y_test,predictions))
print(confusion_matrix(y_test,predictions))

((2425+8)/(2425+6+435+8))*100




