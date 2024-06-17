class Counter:
  def __init__(self, print_increment=None) -> None:
    self.count = 0
    self.print_increment = print_increment

  def increment(self):
    self.count += 1
    if self.print_increment:
      if self.count % self.print_increment == 0:
        print(self.count, 'records processed', end='\r')