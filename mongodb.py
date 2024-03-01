import pymongo 


class MongoDBCollection:
    def __init_(self,
                username,
                password,
                ip_address,
                database_name,
                collection_name): 
        self.username = username
        self.password = password
        self.ip_address = ip_address
        self.collection_name = collection_name
        
        self.client = pymongo.MongoClient(f"mongodb+srv://{username}:{password}@{ip_address}")
        self.db = self.client[database_name]
        self.collection = self.db[collection_name] 
        self.connection_string = f"mongodb+srv://{username}:{password}@{ip_address}/{database_name}.{collection_name}"
    def return_db(self):
        return self.db

    def return_collection(self):
        '''
        Return db which belongs to the db.
        '''
        return self.collection

    def return_num_docs(self, query):
        '''
        Return the number of documents satisfying the given query.
        '''
        return self.collection.count_documents(query)

    def drop_collection(self):
        '''
        Drop the collection
        '''
        return self.collection.drop()

    def find(self, query, projection):
        '''
        Return an iteratatable using query and projection.
        '''
        for item in self.collection.find(query, projection):
            yield item

    def insert_one(self, doc):
        '''
        Insert the given document
        '''
        self.collection.insert_one(doc)

    def insert_many(self, docs):
        '''
        Insert the given documents
        '''
        self.collection.insert_many(docs)

    def update_many(self, filter, update):
        '''
        Update documents satisfying filter with update.
        Both filter and update are dictionaries.
        '''
        self.collection.update_many(filter, update)     
               