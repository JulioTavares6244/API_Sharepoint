def lower_columns(self, columns):    
  for c in columns:
    self.df = self.df.withColumn(c, trim(lower(col(c))))

def upper_columns(self, columns):    
  for c in columns:
    self.df = self.df.withColumn(c, trim(upper(col(c))))

def cast_col_integer(self, columns):    
  for c in columns:
    self.df = self.df.withColumn(c, col(c).cast('integer'))

def fill_nulls(self):    
  self.df = self.df.fillna('0')
  self.df = self.df.fillna(0)

def print_schema(self):    
  self.df.printSchema()

def cast_col_timestamp(self, columns):    
  for c in columns:
    self.df = self.df.withColumn(c, col(c).cast('timestamp'))

def cast_col_date(self, columns):    
  for c in columns:
    self.df = self.df.withColumn(c, col(c).cast('date'))

def cast_col_binary(self, columns):    
  for c in columns:
    self.df = self.df.withColumn(c, col(c).cast('binary'))

def cast_col_boolean(self, columns):    
  for c in columns:
    self.df = self.df.withColumn(c, col(c).cast('boolean'))