from datetime import datetime
from Prediction_Raw_Data_Validation.predictionDataValidation import Prediction_Data_validation
from DataTypeValidation_Insertion_Prediction.DataTypeValidationPrediction import dBOperation
from DataTransformation_Prediction.DataTransformationPrediction import dataTransformPredict
from application_logging import logger

class pred_validation:


    def __init__(self,path):
        self.raw_data = Prediction_Data_validation(path)
        self.dataTransform = dataTransformPredict()
        self.dBOperation = dBOperation()
        self.file_object = open("Prediction_Logs/Prediction_Logs.txt", 'a+')
        self.log_writer = logger.App_Logger()
        
    def pred_validation(self):
        try:
            self.log_writer.log(self.file_object,'start of validating files for training')
            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, noofcolumns = self.raw_data.valuesFromSchema()
            regex = self.raw_data.manualRegexCreation()
            self.raw_data.validationFileNameRaw(regex, LengthOfDateStampInFile, LengthOfTimeStampInFile)
            self.raw_data.validateColumnLength(noofcolumns)
            self.raw_data.validateMissingValuesInWholeColumn()
            
            self.log_writer.log(self.file_object,'Raw data Validation Completed !!')
            self.log_writer.log(self.file_object,'Starting Data Transformation !!')
            
            self.dataTransform.replaceMissingWithNull()
            #self.dataTransform.addQuotesToStringValuesInColumn()
            
            self.log_writer.log(self.file_object,'Data Transformation Completed !!')
            self.log_writer.log(self.file_object,'Start creating Training database and tables by using schema !!')
            
            self.dBOperation.createTableDB('backorder', column_names)
            
            self.log_writer.log(self.file_object, "Table creation Completed!!")
            self.log_writer.log(self.file_object, "Insertion of Data into Table started!!!!")
            
            self.dBOperation.insertIntoTableGoodData('backorder')
            
            self.log_writer.log(self.file_object, "Insertion in Table completed!!!")
            self.log_writer.log(self.file_object, "Deleting Good Data Folder!!!")
            
            #self.raw_data.deleteExistingGoodDataTrainingFolder()
            
            self.log_writer.log(self.file_object, "Good_Data folder deleted!!!")
            self.log_writer.log(self.file_object, "Moving bad files to Archive and deleting Bad_Data folder!!!")
            
            self.raw_data.moveBadFilesToArchiveBad()
            
            self.log_writer.log(self.file_object, "Bad files moved to archive!! Bad folder Deleted!!")
            self.log_writer.log(self.file_object, "Validation Operation completed!!")
            self.log_writer.log(self.file_object, "Extracting csv file from table")
            self.file_object.close()
        except Exception as e:
            print(e)