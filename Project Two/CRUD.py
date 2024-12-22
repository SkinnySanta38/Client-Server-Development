from pymongo import MongoClient
from bson.objectid import ObjectId

class CRUD:
    """ CRUD operations for Animal collection in MongoDB """
    
    def __init__(self, user, pwd, host, port, db, col): # user and pwd pass in user login information
        # Initializing the MongoClient. This helps to 
        # access the MongoDB databases and collections.
        # This is hard-wired to use the aac database, and 
        # the animals collection.
        # Definitions of the connection string variables are
        # unique to the individual Apporto environment.
        #
        #
        # Connection Variables
        #
        USER = user # aacuser
        PASS = pwd  # SNHU1234
        HOST = host # 'nv-desktop-services.apporto.com'
        PORT = port # 33003
        DB = db # 'AAC'
        COL = col # 'animals'
        #
        # Initialize Connection
        #
        self.client = MongoClient('mongodb://%s:%s@%s:%d' % (USER,PASS,HOST,PORT))
        self.database = self.client['%s' % (DB)]
        self.collection = self.database['%s' % (COL)]

        
    # Create method for CRUD.
    def create(self, data):# data should be dictionary
        if data is not None:
            if self.database.animals.find({"animal_id" : data["animal_id"]}).count() != 0: #checks if the same id is present
                raise Exception("Invalid, id is already taken.") # Exception thrown if the id is already taken
            else: # Returns true if the insert was successful and false if not
                return self.database.animals.insert_one(data).acknowledged #returns a Boolean stating if the data was accecpted
        raise Exception("Nothing to save, because data parameter is empty") # Exception thrown if data is empty
           
        
    # Read Method for CRUD
    def read(self, searchterm): # searchterm must be key/value pair
        self.isDict([searchterm]) # Makes sure the searchterm is a dict
        if isinstance(searchterm, dict):
            return self.database.animals.find(searchterm) # Always returns a list, empty if it does not exist
    
    
    # Update Method for CRUD
    def update(self, searchterm, update):
        self.isDict([searchterm, update]) # Makes sure the searchterm and update args are dicts
        count = self.database.animals.find(searchterm).count()
        if count != 0: # Ensures an entry can be found to be updated 
            if count == 1: # Update One
                return self.database.animals.update_one(searchterm, update).modified_count
            else: # Update Many
                return self.database.animals.update_many(searchterm, update).modified_count
        return ("Unable to find a match") # Cannot find a matching entry
            
    # Delete Method for CRUD
    def delete(self, searchterm):
        self.isDict([searchterm]) # Makes sure the searchterm is a dict
        return self.database.animals.delete_many(searchterm).deleted_count
        
    # Method checks if type is dict
    def isDict(self, terms):
        for term in terms:
            if not isinstance(term, dict): # Throw exception if the type is not dict
                raise Exception("Invalid aruguent type! Argument must be of type dict")