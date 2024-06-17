def field_has_value(field, dictionary):
  result = field in dictionary and dictionary[field] != None 
  if result and isinstance(dictionary[field], str):
    result = dictionary[field].strip() != ''
  return result