//Task C.1.1

use FIT5137A1MRDB

//Task C.1.2

db.createCollection("userProfiles")
db.createCollection("placeProfiles")

//Task C.1.3

// Change user profile data types
db.userProfiles.aggregate([
{ $project: {
        _id: { $convert: {input: '$_id', to: 'int' } }, 
        favCuisines:{ $split: ['$favCuisines', ', ']}, 
        favPaymentMethod:{ $split: ['$favPaymentMethod', ', ']}, 
        "location.latitude": { $convert: {input: '$location.latitude', to: 'double' }},  "location.longitude": { $convert: {input: '$location.longitude', to: 'double'}},
        otherDemographics:1,
        "personalTraits.birthYear":{ $convert: {input: '$personalTraits.birthYear', to: 'int' }},
        "personalTraits.height":{ $convert: {input: '$personalTraits.height', to: 'double' }},
        "personalTraits.maritalStatus":1,
        "personalTraits.weight":{ $convert: {input: '$personalTraits.weight', to: 'int' }},
        personality:1,
        "preferences.ambience":1,
        "preferences.budget":1,
        "preferences.dressPreference":1,
        "preferences.smoker":{ $switch: {
            branches: [
              { case: { $eq: [ "$preferences.smoker", "FALSE" ] }, then: false },
              { case: { $eq: [ "$preferences.smoker", "" ] }, then: null},
            ],
            default: { $toBool: "$preferences.smoker" }},},
        "preferences.transport":1,
             }
},
{$out: 'userProfiles',}
         ])

// Change place profile data types
db.placeProfiles.aggregate([
{ $project: {
        _id: { $convert: {input: '$_id', to: 'int' } }, 
        acceptedPaymentModes:{ $split: ['$acceptedPaymentModes', ', ']},
        address:1, 
        cuisines:{ $split: ['$cuisines', ', ']},
        "location.latitude": { $convert: {input: '$location.latitude', to: 'double' }},  "location.longitude": { $convert: {input: '$location.longitude', to: 'double'}},
        parkingArragements:1,
        "placeFeatures.accessibility":1,
        "placeFeatures.alcohol":1,
        "placeFeatures.area":1,
        "placeFeatures.dressCode":1,
        "placeFeatures.franchise":{ $switch: {
            branches: [
              { case: { $eq: [ "$placeFeatures.franchise", "f" ] }, then: false },
            ],
            default: { $toBool: "$placeFeatures.franchise" }},},
        "placeFeatures.otherServices":1,
        "placeFeatures.price":1,
        "placeFeatures.smokingArea":1,
        placeName:1,
             }
},
{$out: 'placeProfiles',}
         ])
         
//Task C.1.4

db.createCollection("openingHours")

// merging collections together
db.placeProfiles.aggregate([
    {
        $lookup:{
            from: "openingHours",
            let :{id:"$_id"},
            pipeline : [
              {$match:
              {$expr:
              {$eq:["$placeID", "$$id"]}}},
              {$project:{placeID:0, _id: 0}}
            ],
            
            as:"openingHours"
        }
    },
{$out: 'placeProfiles',}
])

//Task C.2.1

// querry to show that data is not exist in the database yet
db.placeProfiles.find({_id: 70000}).pretty()

// insert data into the database
db.placeProfiles.insertOne({
  _id: 70000,
  acceptedPaymentModes: ["any"],
  address: {
    city: "San Luis Potosi",
    country: "Mexico",
    state: "SLP",
    street: "Carretera Central Sn"
  },
  cuisines: ["Mexican", "Burgers"],
  parkingArragements: "none",
  placeFeatures: {
    accessibility: "completely",
    alcohol: "No_Alcohol_Served",
    area: "open",
    dressCode: "informal",
    franchise: true,
    otherServices: "Internet",
    price: "medium",
    smokingArea: "not permitted"
  },
  "placeName": "Taco Jacks",
  openingHours: [{
    hours:"09:00-20:00",
    days: "Mon;Tue;Wed;Thu;Fri;"
  },
{
  hours:"12:00-18:00",
  days: "Sat;"
},{
  hours:"12:00-18:00",
  days: "Sun;"
},
]

})

//Task C.2.2

db.userProfiles.update(
   { _id: 1108 },
   {
     $pull: { favCuisines: "Fast_Food", favPaymentMethod: "cash"}, 
   }
   
)

db.userProfiles.update(
   { _id: 1108 },
   {
      $addToSet: { favPaymentMethod: "debit_cards" } , 
   }
   
)

//Task C.2.3

db.userProfiles.deleteOne({_id:1063})

//Task C.3.1

db.userProfiles.count()

//Task C.3.2

db.placeProfiles.count()

//Task C.3.7

db.userProfiles.find({"otherDemographics.employment":"student","preferences.budget":"medium"}).pretty()

//Task C.3.8

// querry to show user who like Bakery cuisine to show data existing in the database
db.userProfiles.find({favCuisines:"Bakery"}).pretty()
// querry to show places with Bakery cuisine to show data existing in the database
db.placeProfiles.find({cuisines:"Bakery"}).pretty()

db.userProfiles.aggregate([{
  $match: {favCuisines:"Bakery"}},
  {$lookup:{
    from: "placeProfiles",
    pipeline: [
      {$match: {cuisines:"Bakery"}},
    ],
    as: "places"

  }}
]).pretty()

//Task C.3.9
db.placeProfiles.find({openingHours:{$elemMatch:{days:"Sun;", hours: {$ne:"00:00-00:00;"}}}, cuisines:{$elemMatch:{$eq:"International"}}}).pretty()

//Task C.3.11
db.userProfiles.aggregate([
    { $group:
     
     {_id: {drinkLevel: "$personality.drinkLevel"}, 
      avgAge: {$avg: {$subtract:[{$year: new Date()}, "$personalTraits.birthYear"]}}}},
    
])

//Task C.3.13

db.userProfiles.aggregate([
    { $match: {favCuisines : "Japanese" }},
    { $match: {"personalTraits.maritalStatus":"single"}},
    { $group:
     
     {_id: {ambience: "$preferences.ambience"}, 
      count: {$sum: 1 }}},
    { $sort: {count: -1}}
]).pretty()


//Task C.3.14

db.placeProfiles.distinct("cuisines", { "cuisines" : { $nin : [""] } })

//Task C.3.15

db.placeProfiles.aggregate([
{
    $project: {
        _id:0,
        placeName:1,
        cuisines: {$switch: {
            branches: [
              { case: { $in: [ "Mexican", "$cuisines" ] }  , then: "serves mexican food" },
            ],
            default:  "doesn't serve mexican food",
            }}
    }
}
])

//Task C.3.16

db.userProfiles.count({$or: [{"preferences.transport":"public"}, {"preferences.transport":"on foot"}]})

//Task C.3.18

// creating compound index to effective querry proceeding
db.placeProfiles.createIndex({"placeFeatures.franchise":1, "placeFeatures.price":1})

// querry to show that data exist in the database
db.placeProfiles.find({"placeFeatures.franchise":true, "placeFeatures.price":"low"})

// final querry, shows only required information for usability
db.placeProfiles.aggregate([
  {$match:{"placeFeatures.franchise":true, "placeFeatures.price":"low"}},
  {$project:{_id:0, placeName: 1, address:1,placeFeatures:1, }}
]).pretty()

//Task C.3.19

db.placeProfiles.aggregate([
    {$unwind:"$cuisines"},
    { $group:
     
     {_id: {cuisines: "$cuisines"}, 
      count: {$sum: 1 }}},
    {$sort:{count:-1}}
    
]).pretty()

