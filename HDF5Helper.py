# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# "Project Name"        :   "Code"                                      #
# "File Name"           :   "HDF5Helper"                                #
# "Author"              :   "rishabhzn200"                              #
# "Date of Creation"    :   "Aug-01-2018"                               #
# "Time of Creation"    :   "9:11 AM"                                   #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


import pandas as pd

class HDF5Helper:
    def __init__(self):
        pass

    def CreateStore(self, storename):
        # Create a new store. Do it once using append mode
        self.store = pd.HDFStore(storename, 'a')
        return self.store

    def GetHDF5Store(self, storename):
        self.store = pd.HDFStore(storename, 'a')
        return self.store

    def ReadHDF5Store(self, storename):
        self.store = pd.HDFStore(storename, 'r')
        return self.store

