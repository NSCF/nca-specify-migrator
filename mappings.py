collectonObjectMapping = {
  "Catalognumber": "catalognumber", 
  "CollNumber": "fieldNumber",
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
  "FullLocalityName": "localityname",
  "Site": "text1",
  "MigrationLatitude": "Lat1Text",
  "MigrationLongitude": "Long1Text",
  "VerbatimElevation": "verbatimelevation"
}

collectingEventMapping = {
  "EventCode": "StationFieldNumber",
  "CollDate": "StartDate",
  "Method": "Method",
  "EventRemarks": "remarks"
}

collectingEventAttributesMapping = {
  "Biome": "text1",
  "Association": "text2",
  "MacroHabitat": "text3",
  "MicroHabitat": "text4"
}

collectingTrip = {
  "Project": "collectingtripname"
}

taxon = {
  "fullname": "fullname",
  "rank": "rank"
}

geography = {
  "geography": "name",
  "geographyRank": "rank"
}