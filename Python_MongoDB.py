from pymongo import MongoClient

# MongoDB connection string
conn_string = "mongodb+srv://Geetu07:Geetu13@cluster.lraefjc.mongodb.net/?retryWrites=true&w=majority&appName=Cluster"

# Connect to MongoDB
client = MongoClient(conn_string)

# Select the database
db = client['airbnb_mart']

# Select the collection
collection = db['airbnb_property_review']


# Insert a document
# insert_result = collection.insert_one({'name': 'John Doe', 'age': 30})
# print(f"Document inserted with id: {insert_result.inserted_id}")



# # Query the collection

# AND CONDITION
# find_query = {'property_type' : 'Apartment', 'room_type': 'Private room'}
# count_results = collection.count_documents(find_query) # Fetching count
# print(count_results)

# coll_data=collection.find(find_query) # Fetching all the documents/rows
# for i in coll_data:
#     print(i)

# find_query = { '$or' : [ {'property_type' : 'House'} , {'property_type' : 'Apartment'} ] } # Using OR CONDITION
# count_results = collection.count_documents(find_query)
# print(count_results)

# find_query = { 'room_type': 'Private room',
#                '$or' : [ {'property_type' : 'House'} , {'property_type' : 'Apartment'} ]
#             } # (condition1) AND ((condition2) or (condition3))
#
# count_results = collection.count_documents(find_query)
# print(count_results)


# find_query = { 'accommodates': { '$gte' : 2} }
# count_results = collection.count_documents(find_query)
# print(count_results)

# count_results = collection.count_documents(find_query)
# print("Total Documents Found : ",count_results)

# Print documents fetched from find query
# find_results = collection.find(find_query)
# for doc in find_results:
#     print(doc)

#
# grp1 = [
#     {
#         "$group": {
#             "_id": "$address.country",  # Field to group by
#             "avg_price": {"$avg": "$price"}  # Field to avg
#         }
#     },
#     {
#         "$project": {
#             "country": "$_id",
#             "avg_price": {"$toDouble": "$avg_price"},
#             "_id": 0
#         }
#     }
# ]

grp2 = [
    {
        "$group": {
            "_id": {
                "country": "$address.country",
                "city": "$address.suburb"
            },
            "avg_price": {"$avg": "$price"}
        }
    },
    {
        "$project": {
            "country": "$_id.country",
            "city": "$_id.city",
            "avg_price": {"$toDouble": "$avg_price"},
            "_id": 0
        }
    }
]
#
#
results = collection.aggregate(grp2)
for result in results:
    print(result)

#
# # Close the connection
# client.close()