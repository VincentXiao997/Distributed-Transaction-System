import json
from collections import namedtuple, deque, OrderedDict
import threading

import time


from types import SimpleNamespace

def messageJsonDecod(messageDict):
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

# class ExecuteMessage:
#     def __init__(self, committable, accounts):
#         self.committable
#         self.accounts

class Transaction:
    def __init__(self, transactionId, clientId):
        self.clientId = clientId
        self.transactionId = transactionId
        self.accounts = {}
        self.locks = {}
        self.operations = deque([])
        self.reply = None
        self.newAccounts = []

class Operation:
    def __init__(self, message):
        self.action = message.action
        self.serverId = message.serverId
        self.accountId = message.accountId
        self.amount = message.amount


class Coordinator:
    def __init__(self, serverId, serverNames, sendMessageToServer, sendMessageToClient):
        self.serverId = serverId
        self.serverNames = serverNames
        self.clientsTransactions = {}
        self.sendMessageToServer = sendMessageToServer
        self.sendMessageToClient = sendMessageToClient

    def receiveClientMessage(self, rawMessage):
        message = json.loads(rawMessage, object_hook=messageJsonDecod)
        # print("receiveClientMessage", message.__dict__)
        clientId = message.clientId
        action = message.action
        if clientId not in self.clientsTransactions:
            if action == "BEGIN":
                # print("BEGIN NEW TRANSACTION", clientId, message.transactionId)
                self.clientsTransactions[clientId] = OrderedDict()
                self.clientsTransactions[clientId][message.transactionId] = Transaction(message.transactionId, clientId)
                threading.Thread(target=self.processTransaction, args=(clientId,)).start()
                # self.replyClient(clientId, True, "OK", message.transactionId)
                # start a thread to process transaction
            else:
                # return value to abort
                pass
        else:
            # while clientId has a processing transaction
            if message.transactionId in self.clientsTransactions[clientId]:
                self.clientsTransactions[clientId][message.transactionId].operations.append(Operation(message))
            else:
                # while the transaction id is different from the current transaction id
                self.clientsTransactions[clientId][message.transactionId] = Transaction(message.transactionId, clientId)
                # threading.Thread(target=self.processTransaction, args=(clientId, message.transactionId)).start()
                # self.replyClient(clientId, True, "OK", message.transactionId)


    def receiveServerMessage(self, rawMessage):
        accountMessage = json.loads(rawMessage, object_hook=messageJsonDecod)
        clientId = accountMessage.clientId
        transactionId = accountMessage.transactionId
        self.clientsTransactions[clientId][transactionId].reply = accountMessage
        

    def processTransaction(self, clientId):
        while True:
            while not self.clientsTransactions[clientId]:
                time.sleep(0.1)

            transactionId = list(self.clientsTransactions[clientId])[0]
            transaction = self.clientsTransactions[clientId][transactionId]
            # print("BEGIN NEW TRANSACTION", clientId, transactionId)
            self.replyClient(clientId, True, "OK", transactionId)
            sleepTime = 0
            while True:
                if sleepTime > 50:
                    self.sendAbortedMessagesToServers(transaction)
                    self.replyClient(transaction.clientId, False, "TIMEOUT, ABORTED", transaction.transactionId)
                    self.clientsTransactions[clientId].pop(transactionId)
                    break
                if transaction.operations:
                    sleepTime = 0
                    if len(transaction.operations) > 1 and transaction.operations[1].action == "ABORT":
                        self.sendAbortedMessagesToServers(transaction)
                        self.replyClient(transaction.clientId, False, "ABORTED", transaction.transactionId)
                        self.clientsTransactions[clientId].pop(transactionId)
                        break
                    elif transaction.operations[0].action in ["COMMIT", "ABORT"]:
                        transaction.reply = AccountMessage(None, None, None, None, True, transactionId, None, None, None)
                    else:
                        self.checkAccountInfo(transaction.operations[0], clientId, transaction.transactionId)
                    while not transaction.reply:
                        time.sleep(0.01)
                        # todo: abort after a limited time
                    accountInfo = transaction.reply
                    transaction.reply = None
                    if accountInfo.lock:
                        operation = transaction.operations.popleft()
                        removeTransaction = self.executeOperation(transaction, operation, accountInfo)
                        if removeTransaction:
                            # print("removeTransaction")
                            self.clientsTransactions[clientId].pop(transactionId)
                            # print("remove Transaction", self.clientsTransactions[clientId])
                            break
                        continue # skip waiting
                time.sleep(0.1)
                sleepTime += 0.1

                
    def checkAccountInfo(self, operation, clientId, transactionId):
        accountName = operation.serverId + "." + operation.accountId
        if accountName in self.clientsTransactions[clientId][transactionId].locks and self.clientsTransactions[clientId][transactionId].locks[accountName] == "WRITE":
            # print("enter here")
            replyMessage = AccountMessage(operation.serverId, operation.accountId, self.clientsTransactions[clientId][transactionId].accounts[accountName], clientId, "WRITE", transactionId)
            self.clientsTransactions[clientId][transactionId].reply = replyMessage
        else:
            acquireMessage = AccountMessage(operation.serverId, operation.accountId, operation.amount, clientId, None, transactionId)
            if operation.action == "BALANCE":
                acquireMessage.lock = "READ"
            else:
                acquireMessage.lock = "WRITE"
            # print(time.time(), acquireMessage.__dict__)
            self.sendMessageToServer(json.dumps(acquireMessage.__dict__), operation.serverId)

    def executeOperation(self, transaction, operation, accountInfo):
        # print("executeOperation", operation.__dict__)
        # print("executeOperation accountInfo", accountInfo.__dict__)
        if operation.action not in ["COMMIT", "ABORT"]:
            accountName = accountInfo.serverId + "." + accountInfo.accountId
            transaction.locks[accountName] = accountInfo.lock
        removeTransaction = False
        if operation.action == "DEPOSIT":
            if accountInfo.newAccount:
                transaction.newAccounts.append(accountName)
            transaction.accounts[accountName] = transaction.accounts.get(accountName, accountInfo.amount) + operation.amount
            self.replyClient(transaction.clientId, True, "OK", transaction.transactionId)
            # print(accountName, transaction.accounts[accountName])
        elif operation.action == "BALANCE":
            # print("BALANCE", transaction.accounts)
            # print("BALANCE", accountInfo.amount)
            if accountInfo.newAccount:
                transaction.accounts[accountName] = 0
                transaction.newAccounts.append(accountName)
                self.sendAbortedMessagesToServers(transaction)
                self.replyClient(transaction.clientId, False, "NOT FOUND, ABORTED", transaction.transactionId)
                removeTransaction = True
            else:
                amount = transaction.accounts.get(accountName, accountInfo.amount)
                transaction.accounts[accountName] = amount
                self.replyClient(transaction.clientId, True, f"{accountName} = {amount}", transaction.transactionId)
        elif operation.action == "WITHDRAW":
            if accountInfo.newAccount:
                transaction.newAccounts.append(accountName)
                transaction.accounts[accountName] = 0
                self.sendAbortedMessagesToServers(transaction)
                removeTransaction = True
                self.replyClient(transaction.clientId, False, "NOT FOUND, ABORTED", transaction.transactionId)
            else:
                transaction.accounts[accountName] = transaction.accounts.get(accountName, accountInfo.amount) - operation.amount
                self.replyClient(transaction.clientId, True, "OK", transaction.transactionId)
        elif operation.action == "COMMIT":
            if self.commitCheck(transaction):
                self.sendCommitMessages(transaction)
            else:
                self.sendAbortedMessagesToServers(transaction)
                self.replyClient(transaction.clientId, False, "ABORTED", transaction.transactionId)
            removeTransaction = True
        elif operation.action == "ABORT":
            self.sendAbortedMessagesToServers(transaction)
            self.replyClient(transaction.clientId, False, "ABORTED", transaction.transactionId)
            removeTransaction = True
        # print("executeOperation", transaction.accounts)
        return removeTransaction
        
    def commitCheck(self, transaction):
        for account in transaction.accounts:
            if transaction.accounts[account] < 0:
                return False
        return True

    def sendCommitMessages(self, transaction):
        # print("$$$$$$$$$$$$$$$$$$$$ enter sendCommitMessages")
        # print("transaction", transaction.__dict__)
        toCommitServers = {}
        for accountName in transaction.accounts:
            accountInfo = accountName.split(".")
            if accountInfo[0] in toCommitServers:
                toCommitServers[accountInfo[0]][accountInfo[1]] = transaction.accounts[accountName]
            else:
                toCommitServers[accountInfo[0]] = {}
                toCommitServers[accountInfo[0]][accountInfo[1]] = transaction.accounts[accountName]
        # print(toCommitServers)
        for serverId in toCommitServers:
            # print("$$$$$$$$$$$$", toCommitServers[serverId])
            message = AccountMessage(None, None, None, None, None, transaction.transactionId, None, True, dict(toCommitServers[serverId]))
            self.sendMessageToServer(json.dumps(message.__dict__), serverId)
        self.replyClient(transaction.clientId, False, "COMMIT OK", transaction.transactionId)


    def sendAbortedMessagesToServers(self, transaction):
        # print("sendAbortedMessagesToServers")
        # print(transaction.newAccounts)
        toRemoveAccountsInServers = {}
        for accountName in transaction.accounts:
            accountInfo = accountName.split(".")
            if accountInfo[0] in toRemoveAccountsInServers:
                if accountName in transaction.newAccounts:
                    toRemoveAccountsInServers[accountInfo[0]][accountInfo[1]] = True
                else:
                    toRemoveAccountsInServers[accountInfo[0]][accountInfo[1]] = False
            else:
                toRemoveAccountsInServers[accountInfo[0]] = {}
                if accountName in transaction.newAccounts:
                    toRemoveAccountsInServers[accountInfo[0]][accountInfo[1]] = True
                else:
                    toRemoveAccountsInServers[accountInfo[0]][accountInfo[1]] = False
        for serverId in toRemoveAccountsInServers:
            # print("$$$$$$$$$$$$", toRemoveAccountsInServers[serverId])
            message = AccountMessage(None, None, None, None, None, transaction.transactionId, None, False, dict(toRemoveAccountsInServers[serverId]))
            # print("$$$$$$$$sendAbortedMessagesToServers")
            # print(message.__dict__)
            
            self.sendMessageToServer(json.dumps(message.__dict__), serverId)


    def replyClient(self, clientId, status, message, transactionId):
        replyMessage = Message(clientId, None, None, None, None, transactionId, status, message)
        self.sendMessageToClient(json.dumps(replyMessage.__dict__), clientId)
