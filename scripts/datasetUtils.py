import os
import glob
import json
import pandas
import subprocess


# Still working on this
class DatasetGroup:
    """DatasetGroup is a class for a group of datasets"""
    def __init__(self, datasetList):
        self.datasetList = datasetList

    def addDatasetToGroup(self, dataset):
        if not dataset.isPackaged:
            self.arePackaged = False
        if dataset not in self.datasetList:
            self.datasetList.append(dataset)


class Dataset(object):
    """Dataset class"""
    def __init__(self, name, webSource, folderPath):
        self.name = name
        self.webSource = webSource
        self.folderPath = folderPath
        self.isPackaged = 'datapackage.json' in os.listdir(folderPath)

    def __str__(self):
        dictForPrint = {
            "Name": self.name,
            "Source": self.webSource,
            "Local Folder Path": self.folderPath,
            "Is packaged": self.isPackaged
        }
        stringToPrint = '{\n' + ',\n'.join(map(lambda key: '\t' + key + ': ' + str(dictForPrint[key]), dictForPrint))
        stringToPrint += '\n}'
        return stringToPrint

    def createDatapackage(self):
        if self.isPackaged:
            print("dataset is already packaged")
        else:
            oldPath = os.getcwd()
            os.chdir(self.folderPath)
            os.system('data init ')
            os.chdir(oldPath)
            self.isPackaged = True
            print("dataset has been succesfully packaged")

    def validateDatapackage(self):
        cmd = ['data', 'validate', self.folderPath]
        output = subprocess.Popen(cmd, stdout=subprocess.PIPE).communicate()[0]
        validationResponse = output.decode("utf-8")
        if 'Your Data Package is valid!' in validationResponse:
            return True
        else:
            print(validationResponse)
            return False

    def loadCsvResources(self):
        csvFilePaths = glob.glob(self.folderPath + '/**/*.csv', recursive=True)
        csvDataDictionary = {}
        for filePath in csvFilePaths:
            csvDataDictionary[filePath] = pandas.read_csv(filePath)
        self.csvDataDictionary = csvDataDictionary

    def loadMetadataJSON(self):
        self.metadataJSON = json.loads(open(self.folderPath + "/datapackage.json", "r").read())

    def pushToDataHubByUser(self, user=''):
        print(('data push ' + self.folderPath + ' --published'))
        if not user.isAuthenticated:
            user.authenticate()
        os.system('data push ' + self.folderPath + ' --published')


class DataHubUser:
    """DataHubUser Class"""
    def __init__(self, name, configJsonFilePath):
        self.name = name
        self.configJsonFilePath = configJsonFilePath
        self.isAuthenticated = False

    def __str__(self):
        return '{\n\tname: ' + self.name + '\n\tconfigJsonFilePath: ' + self.configJsonFilePath + '\n}'

    def printConfigJSON(self):
        print(open(os.environ["HOME"] + '/' + self.configJsonFilePath).read())

    def authenticate(self):
        os.environ["DATAHUB_JSON"] = os.environ["HOME"] + '/' + self.configJsonFilePath
        self.isAuthenticated = True
        print("User ", self.name, " is now authenticated")
