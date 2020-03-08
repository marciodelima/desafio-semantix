#!/usr/bin/env python
# coding: utf-8

#Processamento de Arquivos de LOGs com Spark - 02/2020
#MARCIO DE LIMA
# 
#DADOS Fornecidos
# 
# Fonte oficial do dataset: http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html
# 
# Os dois conjuntos de dados possuem todas as requisições HTTP para o servidor da NASA Kennedy
# Space Center WWW na Flórida para um período específico.
# 
#Colunas 
# Arquivos em ASCII com as colunas:
# 
# Host , um hostname quando possível, caso contrário o endereço de internet se o nome não puder ser  identificado.
# 
# Timestamp no formato "DIA/MÊS/ANO:HH:MM:SS TIMEZONE"
# 
# Requisição (entre aspas)
# 
# Código do retorno HTTP
# 
# Total de bytes retornados
# 
# 

# *********** Atenção: *********** 
# Ambiente de Desenvolvimento desse fonte: Linux, Java JDK 8 , Apache Spark 2.4.3 e Python 3.7

#Importando as bibliotecas
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row


# Define o Spark Context, pois o job será executado via linha de comando com o spark-submit
conf = SparkConf().setMaster("local").setAppName("DesafioSpark")
sc = SparkContext(conf = conf)

# Criando o SQL Context para trabalhar com Spark SQL
sqlContext = SQLContext(sc)

#Versão do Spark utilizada
print(sc.version)

# Criando os RDDs a partir dos arquivos fornecidos, enconding => iso-8859-1
logJulRDD = sc.textFile("dados/NASA_access_log_Jul95", use_unicode=False).map(lambda x: x.decode("iso-8859-1"))
logAgoRDD = sc.textFile("dados/NASA_access_log_Aug95", use_unicode=False).map(lambda x: x.decode("iso-8859-1"))


#Juntando os dois RDDs em 1 único RDD
joinedLinhas = logJulRDD.union(logAgoRDD)

#Contagem de Linhas de LOG
joinedLinhas.count()

# O Join insere 1 linha em branco no final do arquivo, alem disso, o arquivo pode conter linhas inválidas
joinedLinhasL = joinedLinhas.filter(lambda linha: linha != '')

# Limpando a memória
del joinedLinhas, logJulRDD, logAgoRDD

#Persistindo na memória
joinedLinhasL.persist()
joinedLinhasL.cache()

#Função de limpeza e tratamento dos dados, linha a linha
def prepararDados(linha): 
    
    attList = linha.split('\"')
    attList_1 = attList[0].split(" ") 

    try:
        data = attList_1[3].strip()[1:7] if attList_1[3] != '' else '' 
        erro_data = 0
    except:
        data = '01/Jan'
        erro_data = 1
    finally:{}

    try:
        attList_2 = linha.split('/1.0\"')[1].split(" ")
        httpCode = attList_2[1].strip() if attList_2[1] != '-' and attList_2[1] != '' else '0' 
        erro_httpCode = 0
    except:
        httpCode = '0' 
        erro_httpCode = 1
    finally:{}

    try:
        attList_2 = linha.split('/1.0\"')[1].split(" ")
        bytesTransf = attList_2[2].strip() if attList_2[2] != '-' else '0'
        erro_bytes = 0
    except:
        bytesTransf = '0'
        erro_bytes = 1
    finally:{}

    host = attList_1[0].strip() if attList_1[0] != '' else '' 

    try:
        url = attList[1].replace("GET ","").replace(" HTTP/1.0","").strip()
        erro_url = 0
    except:
        url = ''
        erro_url = 1
    finally:{}
    
    valores = Row(Host = host, Data = data, HttpCode = int(httpCode), Bytes = int(bytesTransf), Url = url, ErroConversaoData=erro_data,ErroConversaoUrl=erro_url, ErroConversaoHttp=erro_httpCode, ErroConversaoBytes=erro_bytes)
    return valores    


# Aplicando a função em todo o dataSet
joinedLinhasLimpo = joinedLinhasL.map(prepararDados)

#Persistindo na memória
joinedLinhasLimpo.persist()
joinedLinhasLimpo.cache()

# Criando DataFrame
df = sqlContext.createDataFrame(joinedLinhasLimpo)

# Criando DataFrame
df = sqlContext.createDataFrame(joinedLinhasLimpo)

# Zerando valores NA , caso existam
data_df = df.fillna(0)

# Mostrando e colocando no cache os dados
data_df.persist()
data_df.cache()

# Limpando a memória
del joinedLinhasLimpo, df

# Registrando o dataframe como uma Temp Table para a execução dos SQLs
data_df.createOrReplaceTempView("linhasTB")

#Questões

#1) Número de hosts únicos

joinedLinhasL.map(lambda linha: linha.split(" ")[0]).distinct().count()

#Resposta: 137979 hosts

#2) O total de erros 404

sqlContext.sql("select count(Host) as resultado from linhasTB where HttpCode = '404'").show()


#Resposta: 20698 erros

#3) Os 5 URLs que mais causaram erro 404

# Executando SQL - Top 5
consulta = sqlContext.sql("select Url as url , count(HttpCode) as resultado from linhasTB where HttpCode = '404' GROUP BY Url ORDER BY resultado desc").limit(5)

#Resposta: 
# 
# /pub/winvn/readme.txt
# 
# /pub/winvn/release.txt
# 
# /shuttle/missions/STS-69/mission-STS-69.html
# 
# /shuttle/missions/sts-68/ksc-upclose.gif
# 
# /history/apollo/a-001/a-001-patch-small.gif

#4) Quantidade de erros 404 por dia

# Executando SQL - Quantidade de erros 404 por dia
sqlContext.sql("select Data, count(HttpCode) as resultado from linhasTB where HttpCode = '404' GROUP BY Data ORDER BY Data").show()

#Resposta: 
# +------+---------+
# 
# |  Data|resultado|
# 
# +------+---------+
# 
# |01/Aug|      242|
# 
# |01/Jul|      315|
# 
# |02/Jul|      291|
# 
# |03/Aug|      300|
# 
# |03/Jul|      473|
# 
# |04/Aug|      343|
# 
# |04/Jul|      355|
# 
# |05/Aug|      232|
# 
# |05/Jul|      492|
# 
# |06/Aug|      371|
# 
# |06/Jul|      633|
# 
# |07/Aug|      526|
# 
# |07/Jul|      568|
# 
# |08/Aug|      386|
# 
# |08/Jul|      302|
# 
# |09/Aug|      277|
# 
# |09/Jul|      342|
# 
# |10/Aug|      312|
# 
# |10/Jul|      392|
# 
# |11/Aug|      260|
# 
# +------+---------+
# 
# only showing top 20 rows
# 

#5) O total de bytes retornados

data_df.groupBy().sum().collect()[0][0]

#Resposta: 65401233313 bytes

# Executando SQL com a Resposta da Soma Total de Bytes
sqlContext.sql("select sum(Bytes) as resultado from linhasTB").show()

# Limpando a memória
del joinedLinhasL, data_df

#PROBLEMAS NAS CONVERSÕES DE LINHAS##
# 
# Necessário análise do arquivo para verificar os motivos e as inconsistências. Foi decidido por mim, nesse desafio ignorar essas linhas e processar as demais. 

sqlContext.sql("select count(*) as erros from linhasTB WHERE ErroConversaoBytes > 0 or ErroConversaoData > 0 or ErroConversaoHttp >0 or ErroConversaoUrl > 0 ").show()

#Resposta: 6539 erros na conversão

#Percentual de erros na conversao => (6539 / 3461613) * 100 => 0.18%

sqlContext.sql("select * from linhasTB WHERE ErroConversaoBytes > 0 or ErroConversaoData > 0 or ErroConversaoHttp >0 or ErroConversaoUrl > 0 ").show()

#Fechando o contexto
sc.stop()

# FIM
# OBRIGADO

