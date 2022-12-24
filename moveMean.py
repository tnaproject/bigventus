import numpy as np

import pandas as pd

def moveMean(arr,f,b):

    oldArray=arr

    newArray=oldArray.copy()

    for rowNo in range(0,newArray.shape[0]):

        if rowNo<=b:

            addCount=0

            tmpArray=oldArray[rowNo].copy()

            for forwardNo in range(1,f+1):

                tmpArray+=oldArray[rowNo+forwardNo]

                addCount+=1

            newArray[rowNo]=tmpArray/(addCount+1)

        elif rowNo+f>newArray.shape[0]-1:

            addCount=0

            b=-1*b

            tmpArray=oldArray[rowNo]

            for forwardNo in range(b,-1):

                tmpArray+=oldArray[rowNo+forwardNo]

                addCount+=1

            newArray[rowNo]=tmpArray/(addCount+1)

    else:

            addCount=0

            tmpArray=oldArray[rowNo]

            b=-1*b

            for moveNo in range(b,f):

                if moveNo!=0:

                    tmpArray[rowNo]+=oldArray[rowNo+moveNo]

                    addCount+=1

            newArray[rowNo]=tmpArray/(addCount+1)

    return newArray
