import json
from types import SimpleNamespace

def messageJsonDecod(messageDict):
    return SimpleNamespace(**messageDict)


class AccountMessage:
    def __init__(self, serverId, accountId, amount, clientId, lock, transactionId, newAccount=None, committable=None, accounts=None):
        self.serverId = serverId
        self.accountId = accountId
        self.amount = amount
        self.clientId = clientId
        self.lock = lock
        self.transactionId = transactionId
        self.newAccount = newAccount
        self.committable = committable
        self.accounts = accounts
class Server:
    def __init__(self, serverId, coordinatorId, sendMessageToServer):
        # accounts: {account_id: 
        #               amount: int,
        #               has_wlock: boolean,
        #               locks: [transaction_id]
        #           }
        self.serverId = serverId
        self.sendMessageToServer = sendMessageToServer
        self.accounts = {}
        self.coordinatorId = coordinatorId

    def receiveServerMessage(self, rawMessage):
        message = json.loads(rawMessage, object_hook=messageJsonDecod)
        # print("receive Server Message", message.__dict__)
        if message.committable == None:
            self.checkAccount(message)
        else:
            if message.committable:
                self.commitTransaction(message)
            else:
                self.abortTransaction(message)
        # print("$$$$$$$", self.accounts)
    
    def commitTransaction(self, message):
        # print("commit Transaction", message.__dict__)
        transactionId = message.transactionId
        message.accounts = vars(message.accounts)
        for accountName in message.accounts:
            if self.accounts[accountName]["has_wlock"]:
                self.accounts[accountName]["amount"] = message.accounts[accountName]
                self.accounts[accountName]["has_wlock"] = False
                self.accounts[accountName]["locks"].remove(transactionId)
                if len(self.accounts[accountName]["locks"]) != 0:
                    print("ERROR!!!!!!!!!!!!!!!!!!!!!!")
                    print(accountName, "locks num is not 0: ", len(self.accounts[accountName]["locks"]))
            else:
                self.accounts[accountName]["locks"].remove(transactionId)
        # print("**Commited", self.accounts)

    def abortTransaction(self, message):
        # print("***ABORT")
        transactionId = message.transactionId
        message.accounts = vars(message.accounts)
        for accountName in message.accounts:
            if message.accounts[accountName]:
                self.accounts.pop(accountName)
            else:
                if self.accounts[accountName]["has_wlock"]:
                    self.accounts[accountName]["has_wlock"] = False
                    self.accounts[accountName]["locks"].remove(transactionId)
                    if len(self.accounts[accountName]["locks"]) != 0:
                        print("ERROR!!!!!!!!!!!!!!!!!!!!!!")
                        print(accountName, "locks num is not 0: ", len(self.accounts[accountName]["locks"]))
                else:
                    self.accounts[accountName]["locks"].remove(transactionId)
        # print("**Aborted", self.accounts)

    def checkAccount(self, message):
        # print("&&&&&&&", self.accounts)
        # print("^^^^^^^", message.__dict__)
        has_wlock = True if message.lock == "WRITE" else False
        if message.accountId not in self.accounts: # new account
            self.accounts[message.accountId] = {"amount": 0, "has_wlock": has_wlock, "locks": [message.transactionId]}
            reply = AccountMessage(self.serverId, message.accountId, 0, message.clientId, message.lock, message.transactionId, True)
        else:
            if self.accounts[message.accountId]["has_wlock"] == True: # try to read/write account that has wlock, reject
                reply = AccountMessage(self.serverId, message.accountId, None, message.clientId, None, message.transactionId)
            else:
                if message.lock == "WRITE":
                    # check if it's promotion request
                    if len(self.accounts[message.accountId]["locks"]) == 1 and message.transactionId == self.accounts[message.accountId]["locks"][0]:
                        lock = message.lock
                        self.accounts[message.accountId]["has_wlock"] = True
                    elif len(self.accounts[message.accountId]["locks"]) == 0:
                        lock = message.lock
                        self.accounts[message.accountId]["has_wlock"] = True
                        self.accounts[message.accountId]["locks"].append(message.transactionId)
                    else:
                        lock = None
                    reply = AccountMessage(self.serverId, message.accountId, self.accounts[message.accountId]["amount"], message.clientId, lock, message.transactionId)
                else:
                    if message.transactionId not in self.accounts[message.accountId]["locks"]:
                        self.accounts[message.accountId]["locks"].append(message.transactionId)
                    reply = AccountMessage(self.serverId, message.accountId, self.accounts[message.accountId]["amount"], message.clientId, message.lock, message.transactionId)
        self.sendMessageToServer(json.dumps(reply.__dict__))
        # print(reply.__dict__)
        # print(self.accounts)
    
