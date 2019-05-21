# -*- coding: utf-8 -*-
"""
Created on Tue Oct 23 13:23:40 2018

@author: mabotula
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import KFold
from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.svm import SVC
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis

filename = 'C:/Users/mabotula/Documents/Learning/DeepLearning/Capgemini_ML_DL/diabetes.csv'
names = ['preg', 'plas', 'pres', 'skin', 'test', 'mass', 'pedi', 'age', 'class']
dataset = pd.read_csv(filename,names = names)
dataset.columns
dataset.groupby('class').size()
x = dataset.iloc[:,0:8]
y = dataset.iloc[:,8]
models = []
models.append (('LR',LogisticRegression()))
models.append (('DT',DecisionTreeClassifier()))
models.append (('RF',RandomForestClassifier()))
models.append (('KNN',KNeighborsClassifier()))
models.append (('NB',GaussianNB()))
models.append (('SVM',SVC()))

results = []
names = []
scoring = 'accuracy'
for name,model in models:
    kfold = KFold(n_splits=10, random_state= 7)
    cv_results = cross_val_score(model,x,y,cv = kfold , scoring= scoring )
    results.append(cv_results)
    names.append(name)
    msg = "%s: %f (%f)" % (name, cv_results.mean() , cv_results.std())
    print(msg)
    




fig = plt.figure()
fig.suptitle('Algorithm Comparison')
ax = fig.add_subplot(111)
plt.boxplot(results)
ax.set_xticklabels(names)
plt.show()















