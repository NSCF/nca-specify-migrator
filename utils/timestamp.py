from datetime import datetime

def get_timestamp():
  now = datetime.now()
  formatted = now.strftime('%Y-%m-%d %H:%M:%S')
  return formatted