# Importando o módulo re
# - Utilizado para trabalhar com expressões regulares
import re

# Importando o módulo apache_beam
# - Utilizado para processar pipeline de dados
import apache_beam as beam

# Importando o módulo ReadFromText
# - Utilizado para realizar a leitura de arquivos de texto
from apache_beam.io import ReadFromText


# Importando o módulo WriteToText
# - Utilizado para realizar a escrita de arquivos texto
from apache_beam.io import WriteToText


# Importando o módulo WriteToParquet
# - Utilizado para realizar a escrita de arquivos parquet
from apache_beam.io import WriteToParquet


# Importando o módulo pyarrow
# - Utilizado para armazenar, processar e mover dados em alta velcoidade
import pyarrow


# Importando o módulo PipelineOptions
# - Utilizado para setar configurações do nosso pipeline de dados
from apache_beam.options.pipeline_options import PipelineOptions


# Criando a variável pipeline_options
# - O método PipelineOptions irá configurar o nosso pipeline de dados
pipeline_options = PipelineOptions(argv=None)


# Criando a variável pipeline
# - O método Pipeline(options = pipeline_options) irá inicializar o nosso pipeline com as configurações definidas pela variável pipeline_options
pipeline = beam.Pipeline(options=pipeline_options)


# Criando a variável colunas_dengue
# - A variável colunas_dengue representa todas as colunas do nosso arquivo de texto casos_dengue.txt
colunas_dengue = [
                'id',
                'data_iniSE',
                'casos',
                'ibge_code',
                'cidade',
                'uf',
                'cep',
                'latitude',
                'longitude']


# Criando a função texto_para_lista
# - A função recebe um arquivo texto e um delimitador
# - A função retorna uma lista
def texto_para_lista(linhas, delimitador='|'):
    return linhas.split(delimitador)


# Criando a função lista_para_dicionario
# - A função recebe duas listas (linhas, colunas)
# - A função retorna um dicionário de dados
def lista_para_dicionario(linhas, colunas):
    return dict(zip(colunas, linhas))


# Criando a função trata_datas
# - A função recebe um dicionário de dados e cria o campo ano_mes com base na coluna data_iniSE
# - A função retorna o mesmo dicionário de dados acrescido do campo ano_mes
def trata_datas(dicionario):
    dicionario['ano_mes'] = '-'.join(dicionario['data_iniSE'].split('-')[:2])
    return dicionario


# Criando a função chave_uf
# - A função recebe um dicionário de dados
# - A função retorna uma tupla com o estado (UF) e o elemento (UF, dicionário de dados)
def chave_uf(dicionario):
    chave = dicionario['uf']
    return (chave, dicionario)


# Criando a função descompacta_casos_dengue
# - A função recebe uma tupla no formato ('Estado', [{}, {}, {}])
# - A função retorna uma tupla no formato ('Estado-ano-mes', Quantidade de casos sumarizado)
def descompacta_casos_dengue(tupla):
    # Uf armazenará os estados ('Estado')
    # Registros acumulará as quantidades de casos (somatório de [{}, {}, {}])
    uf, registros = tupla
    for registro in registros:
        # Usando o método search para buscar apenas registros numéricos (\d) nos registros
        if bool(re.search(r"\d", registro['casos'])):
            # Usando o yield para retornar todos os valores dentro do for
            yield (f"{uf}-{registro['ano_mes']}", float(registro['casos']))
        else: 
            yield (f"{uf}-{registro['ano_mes']}", 0.0)


# Criando a função chave_uf_ano_mes
# - A função recebe uma lista com ano-mes-dia, valor e uf
# - A função retorna uma tupla contendo uma chave (uf-ano-mes) e o valor (chuva em mm)
def chave_uf_ano_mes_de_lista(lista):
    # Cada elemento da lista é passado para respectiva variável
    anomesdia, mm_chuva, uf = lista
    chave_uf_anomes = f"{uf}-{anomesdia[0:7]}"
    # Tratando os casos de valores negativos
    if float(mm_chuva) < 0:
        mm_chuva = 0.0
    else:
        mm_chuva = float(mm_chuva)
    return (chave_uf_anomes, mm_chuva)


# Criando a função arredonda
# - A função recebe uma tupla com ano-mes e mm de chuva
# - A função retorna uma tupla com ano-mes e mm de chuva arredondado
def arredonda(tupla):
    ano_mes, mm = tupla
    return (ano_mes, round(mm, 1))


# Criando a função filtra_campos_vazios
# - A função recebe uma tupla (chave, {chuvas : [], dengue : []})
# - A função retorna uma tupla (chave, {chuvas : [], dengue : []}) sem elementos vazios
def filtra_campos_vazios(tupla):
    # Cada elemento da tupla é passado para respectiva variável
    ano_mes, chuva_dengue = tupla
    # A função all retorna True caso os valores sejam válidos 
    if all([chuva_dengue['chuvas'], chuva_dengue['dengue']]):
        return True
    else:
        return False


# Criando a função descompactar_colunas_csv
# - A função recebe uma tupla (uf-ano-mes, {chuvas : [], dengue : []})
# - A função retorna uma string uf;ano;mes;chuva;dengue
def descompactar_colunas_csv(tupla):
    uf_ano_mes, chuva_dengue = tupla
    uf, ano, mes = uf_ano_mes.split("-")
    chuva, dengue = chuva_dengue['chuvas'][0], chuva_dengue['dengue'][0]
    return f"{uf};{ano};{mes};{chuva};{dengue}"


# Criando a função descompactar_colunas_parquet
# - A função recebe uma string uf;ano;mes;chuva;dengue
# - A função retorna um dicionário (colunas, [uf : UF, ano : 0000, mes : 00, chuva : 0.0, dengue : 0.0])
def descompactar_colunas_parquet(string_csv):
    uf, ano, mes, chuva, dengue = string_csv.split(";")
    colunas = ['uf', 'ano', 'mes', 'mm_chuva', 'casos_dengue']
    return dict(zip(colunas, [str(uf), int(ano), int(mes), float(chuva), float(dengue)]))


# Criando a PCollection dengue
# - PCollection é uma coleção que possui etapas, ou seja, uma variável que armazena etapas do pipeline de dados
# - A PCollection dengue é responsável por:
#   - Ler o dataset de dengue
#   - Converter o arquivo texto para lista
#   - Converter a lista para um dicionário de dados
#   - Criar o campo ano_mes
#   - Criar a chave utilizando o campo uf
#   - Agrupar os casos de dengue por estado
#   - Descompactar os casos de dengue por estado e ano_mes
#   - Sumarizar os casos de dengue por estado e ano_mes
#   - Apresentar os resultados
dengue = (
    pipeline
    | "Leitura do arquivo texto de dengue" >> ReadFromText('casos_dengue.txt', skip_header_lines=1)
    | "Converte o arquivo texto para lista" >> beam.Map(texto_para_lista)
    | "Converte a lista para dicionário de dados" >> beam.Map(lista_para_dicionario, colunas_dengue)
    | "Cria o campo ano_mes" >> beam.Map(trata_datas)
    | "Cria chave utilizando o campo uf" >> beam.Map(chave_uf)
    | "Agrupa os casos de dengue por estado" >> beam.GroupByKey()
    | "Descompacta os casos de dengue por estado e ano_mes" >> beam.FlatMap(descompacta_casos_dengue)
    | "Sumariza os casos de dengue por estado e ano_mes" >> beam.CombinePerKey(sum)
    # | "Mostrar resultados dengue" >> beam.Map(print)
)


# Criando a PCollection chuva
# - PCollection é uma coleção que possui etapas, ou seja, uma variável que armazena etapas do pipeline de dados
# - A PCollection chuva é responsável por:
#   - Ler o dataset de chuva
#   - Converter o arquivo texto para lista
#   - Criar a chave uf-ano-mes e o valor de mm de chuvas
#   - Sumarizar os registros de mm de chuvas pela chave uf-ano-mes
chuvas = (
    pipeline 
    | "Leitura do arquivo csv de chuvas" >> ReadFromText("chuvas.csv", skip_header_lines=1)
    | "Converte o arquivo csv para lista" >> beam.Map(texto_para_lista, delimitador = ",")
    | "Cria a chave uf-ano-mes e o valor de mm de chuvas" >> beam.Map(chave_uf_ano_mes_de_lista)
    | "Sumariza os registros de mm de chuvas pela chave uf-ano-mes" >> beam.CombinePerKey(sum)
    | "Arrendondar resultados de chuvas" >> beam.Map(arredonda)
    # | "Mostrar resultados chuva" >> beam.Map(print)
)


# Criando a PCollection resultado
# - PCollection é uma coleção que possui etapas, ou seja, uma variável que armazena etapas do pipeline de dados
# - A PCollection resultado é responsável por:
#   - Unir os dados das PCollections pela chave uf-ano-mes e os valores de chuvas e dengue no formato (chave, {chuva : [], dengue :[]})
#   - Filtrar os campos vazios
#   - Descompactar as colunas
#   - Descompactar a lista para formato csv
#   - Descompactar a lista para o formato parquet
resultado = (
    ({"chuvas" : chuvas, "dengue" : dengue})
    | "Mesclando as PCollections" >> beam.CoGroupByKey()
    | "Filtrando os dados vazios" >> beam.Filter(filtra_campos_vazios)
    | "Descompactando as colunas arquivo csv" >> beam.Map(descompactar_colunas_csv)
    # | "Descompactando as colunas arquivo parquet" >> beam.Map(descompactar_colunas_parquet)
    # | "Mostrar resultados da união" >> beam.Map(print)

)


# Criando a variável colunas
# - A variável irá armazenar o nome de cada coluna do arquivo csv
colunas = "uf;ano;mes;mm_chuva;casos_dengue"


# Exportando o resultado em formato csv
# - A string resultado será exportada no formato csv
resultado | "Criar arquivo no formato csv" >> WriteToText('resultado_csv', file_name_suffix = ".csv", header = colunas)


# Criando o schema para ser passado para o arquivo parquet
# - O schema é uma espécie de organização de dados (nome e tipos)
# schema = [('uf', pyarrow.string()),
#           ('ano', pyarrow.int32()),
#           ('mes', pyarrow.int32()),
#           ('mm_chuva', pyarrow.float32()),
#           ('casos_dengue', pyarrow.float32())
#           ]


# Exportando o resultado em formato parquet
# - A string resultado será exportada no formato parquet
# resultado | "Criar arquivo no formato parquet" >> WriteToParquet('resultado_parquet', file_name_suffix=".parquet", schema = pyarrow.schema(schema))


# Rodando o pipeline de dados
pipeline.run()