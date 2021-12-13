import json
from collections import namedtuple
from types import SimpleNamespace
import time

def messageJsonDecod(messageDict):
    # return namedtuple('Message', messageDict.keys())(*messageDict.values())
    return SimpleNamespace(**messageDict)

class Message:
    def __init__(self, clientId=None, action=None, serverId=None, accountId=None, amount=None, transactionId=None, status=None, message=None):
        self.clientId = clientId
        self.action = action
        self.serverId = serverId
        self.accountId = accountId
        self.amount = amount
        self.transactionId = transactionId
        self.status = status
        self.message = message

class Client:
    def __init__(self, clientId, sendMessage):
        self.clientId = clientId
        self.sendMessage = sendMessage
        self.isBegin = False
        self.transactionId = None

    def validator(self, userInput):
        # isBegin True   BEGIN   ABORT  COMMIT xxx
        # isBegin        ignore  False  False
        # validator      False   True   True   True 
        # isBegin False  BEGIN xxx
        # isBegin        True 
        # validator      True  F       
        if self.isBegin:
            if userInput == "ABORT" or userInput == "COMMIT":
                self.isBegin = False
            if userInput == "BEGIN":
                return False
            return True
        elif userInput == "BEGIN":
            self.isBegin = True
            self.transactionId = str(time.time()) + self.clientId
            return True
        return False

    def userInput(self, userInput):
        # print(userInput)
        # print(self.isBegin)
        userInput = userInput.strip()
        isValid = self.validator(userInput)
        # print("isValid", isValid)
        if not isValid:
            return
        # print(userInput in ["BEGIN", "COMMIT", "ABORT"])
        if userInput in ["BEGIN", "COMMIT", "ABORT"]:
            # print("here")
            message = Message(clientId=self.clientId, action=userInput, transactionId=self.transactionId)
        else:
            userInput = userInput.split(" ")
            userInput[1] = userInput[1].split(".")
            # print(47, userInput)
            message = Message(clientId=self.clientId, 
                              action=userInput[0],
                              serverId=userInput[1][0],
                              accountId=userInput[1][1],
                              transactionId=self.transactionId)
            if len(userInput) == 3:
                message.amount=int(userInput[2])
        # print(message.__dict__)
        self.sendMessage(json.dumps(message.__dict__))
        # time.sleep(1)
    
    def receiveMessage(self, message):
        message = json.loads(message, object_hook=messageJsonDecod)
        if message.transactionId == self.transactionId:
            self.isBegin = message.status
        print(message.message)



    # need a function to set isBegin False

        

        

            
    