# Mapping dictionary property fields below are case sensitive and must exactly match the fields in the input dataset.
# Property values (i.e. database field names) are not, but are used in the script so stick to all lowercase as a convention.

collectonObjectMapping = {
  "Catalognumber": "catalognumber", 
  "CollNumber": "fieldNumber",
  "MicroHabitat": "text7",
  "DNA Code": "text5",
  "BOLD": "text6",
  "Notes": "remarks"
}

collectionObjectAttributesMapping = {
  "Males": "integer1",
  "SubadMales": "integer3",
  "Females": "integer2",
  "SubadFemales": "integer4",
  "ImMales": "integer5",
  "ImFemales": "integer6",
  "ImmIndet": "integer9"
}

determinationMapping = {
  "TypeStatus": 'typestatusname',
  "detQualifier": "qualifier",
  "detSuffix": "text2",
  "certainty": "confidence",
  "DetDate": "determineddate",
  "DetRemarks": "remarks"
}

localityMapping = {
  "FullLocalityName": "localityname", # this db field is required, must always be here
  "Site": "text1",
  "VerbatimElevation": "verbatimelevation"
}

collectingEventMapping = {
  "EventCode": "stationfieldnumber",
  "CollDate": "startdate",
  "Method": "method",
  "EventRemarks": "remarks"
}

collectingEventAttributesMapping = {
  "Biome": "text1",
  "Association": "text2",
  "MacroHabitat": "text3"
}

determinerMapping = {
  "DetBy_FirstName": "firstname",
  "DetBy_LastName": "lastname",
  "DetBy_Initials": "initials"
}

collectingTripMapping = {
  "Project": "collectingtripname"
}

taxonMapping = {
  "fullname": "fullname",
  "rank": "rank"
}

geographyMapping = {
  "geography": "name",
  "geographyRank": "rank"
}

