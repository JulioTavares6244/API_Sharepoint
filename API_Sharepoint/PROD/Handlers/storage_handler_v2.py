class Storage():
  
  def __init__(self, name, path=''):
    self.key = '0000000000000000000000000000000000000000000/00000000000000/000000000000000000000000000'
    self.fs_key = "fs.azure.account.key.yourdatalake.dfs.core.windows.net"
    self.name = name
    self.df = None
    self.xml = ''
    self.project_path = ''
    
  def set_path(self, path):
    self.project_path = '{}/{}'.format(self.project, path)    
    #print('Caminho configurado para: {}\n'.format(self.project_path))
    
    
    
  #----------------------- FUNÇÕES DE LEITURA ---------------------
  def read_from(self, camada, tipo_arquivo='parquet'):
    '''
    Lê arquivo em um formato
    do container de uma camada
    
    tipo_arquivo: json, parquet, delta
    '''

    file_location = "abfss://{0}@yourdatalake.dfs.core.windows.net/{1}/{2}"\
                    .format(camada, self.project_path, self.name)

    fs_key = "fs.azure.account.key.yourdatalake.dfs.core.windows.net"
    spark.conf.set(self.fs_key, self.key)

    try: 
      if tipo_arquivo == 'json':
        self.df = spark.read.json(file_location)
      else:
        self.df = spark.read.format(tipo_arquivo).load(file_location)
      print('Sucesso ao ler tabela {} em\n{}.'.format(self.name.upper(), file_location))
    except:
      print('Erro ao ler tabela {}\n{}:\n{}'.format(self.name.upper(), file_location, sys.exc_info()[1]))

  
     
  #----------------------- FUNÇÕES DE ESCRITA ---------------------
  def write_on(self, camada, tipo_arquivo='parquet'):
    '''
    Escreve arquivo em um formato
    do container de uma camada
    
    tipo_arquivo: json, parquet, delta
    '''

    
    file_location = "abfss://{0}@yourdatalake.dfs.core.windows.net/{1}/{2}"\
                    .format(camada, self.project_path, self.name)

    fs_key = "fs.azure.account.key.yourdatalake.dfs.core.windows.net"
    spark.conf.set(fs_key, self.key)

    try:
      if tipo_arquivo == 'json':
         self.df.write.json(file_location).mode('overwrite')
      else:
        self.df.write.format(tipo_arquivo).mode('overwrite').option("overwriteSchema", "true").save(file_location)      
        
    except:
      print('Erro ao escrever tabela {}.{}:\n{}'.format(camada, self.name.upper(), sys.exc_info()[1]))
      
  def save_on_databricks(self):
    '''
    Salva a tabela na parte Data do Databricks
    para validação dos dados
    '''

    try:
      self.df.write.format("parquet").mode('overwrite').option("overwriteSchema", "true").saveAsTable(self.name)
      print('Sucesso ao salvar tabela {} no Databricks.'.format(self.name.upper()))

    except:
      print('Erro ao escrever tabela {}:\n{}'.format(self.name.upper(), sys.exc_info()[1]))      
      
  def read_from_databricks(self):
    '''
    Lê a tabela na parte Data do Databricks
    para validação dos dados
    '''

    try:
      self.df = spark.sql(f'SELECT * FROM {self.name}')
      print('Sucesso ao ler tabela {} do Databricks.'.format(self.name.upper()))

    except:
      print('Erro ao ler tabela {}:\n{}'.format(self.name.upper(), sys.exc_info()[1])) 
