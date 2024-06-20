from field_has_value import field_has_value

def dict_is_empty(dictionary):
  if len(dictionary.keys()) == 0:
    return True
  return not all(list(map(lambda key: field_has_value(key, dictionary), dictionary.keys()))) 